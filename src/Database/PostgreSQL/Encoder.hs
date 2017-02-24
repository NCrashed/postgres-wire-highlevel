{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
module Database.PostgreSQL.Encoder(
    Encoder(..)
  , runEncoder
  , ToPg(..)
  , ToPrimitive(..)
  , ToArray(..)
  , primitive
  , array
  , arrayNull
  , arrayMay
  ) where

import Control.Monad
import Control.Monad.ST.Strict
import Data.Functor.Identity
import Data.Int
import Data.Map (Map)
import Data.Maybe
import Data.Monoid
import Data.Proxy
import Data.Store (poke)
import Data.Store.Core
import Data.Vector (Vector)
import Data.Word
import Database.PostgreSQL.Protocol.Types
import qualified Data.Array.Repa as R
import qualified Data.Array.Repa.Eval as R
import qualified Data.Array.Repa.Repr.Vector as R
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Foldable as F
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
import qualified Database.PostgreSQL.Protocol.Store.Encode as PE

-- | Name of composite type in postgres. We need the info
type CompositeName = T.Text

-- | Encoder structure that holds all info, how to encode a type for binary protocol
-- of postgresql.
data Encoder =
    -- | Primitive encoder that can be used as array element.
      Encoder'Primitive !EncoderPrimitive
    -- | Encoder of array of given element
    | Encoder'Array !EncoderArray

-- | Holds info how to serialise primitive PostgreSQL value. The struct covers
-- native types, user defined base types and composites.
data EncoderPrimitive = EncoderPrimitive {
    -- | OID of type
    encoderPrimOids   :: !(Either CompositeName (Oid, Oid))
    -- | Either a Null 'Nothing' or poker with size of required buffer
  , encoderPrimPoke   :: !(Maybe (Either [Encoder] PE.Encode))
  }

-- | Holds info how to serialise PostgreSQL arrays
data EncoderArray = EncoderArray {
    -- | Array oids info, either known element and array type or composite type
      encoderArrayElem    :: !(Either CompositeName (Oid, Oid))
    -- | Dimensions of array
    , encoderArrayDims    :: !(Vector ArrayDimension)
    -- | Serialised elements of array
    , encoderArrayPayload :: !(Vector EncoderPrimitive)
    }
  -- | Special case for NULL array
  | EncoderNullArray {
      -- | Encoder for element (we need oids or name from the field)
      encoderNullArrayElem  :: !(Either CompositeName (Oid, Oid))
    }

-- | Specification of array dimension
data ArrayDimension = ArrayDimension {
  -- | Size of dimension
    arrayElements :: Word32
  -- | Start index of the dimension. Almost always 1.
  , arrayDimStart :: Word32
  }

-- | Speical map that stores mapping from type name to oid
data OidsMap = OidsMap {
  -- | Map that stores oids for composite types
  oidsPlain :: Map CompositeName Oid
  -- | Map that stores oids for array types for given name of composite type
, oidsArray :: Map CompositeName Oid
}

-- | Execute encoder action with given mapping of custom composite types.
--
-- 'Left' as result means that given composite OID is unknown, that it is
-- not registered.
runEncoder :: OidsMap -> Encoder -> Either CompositeName (Oid, Maybe BS.ByteString)
runEncoder oidsMap e = do
  (oid, mpoker) <- pokeEncoder oidsMap e
  return (oid, PE.runEncode <$> mpoker)

-- | Helper that constructs poke encoder for primitives with required buffer size
pokePrimitive :: OidsMap -> EncoderPrimitive -> Either CompositeName (Oid, Maybe PE.Encode)
pokePrimitive oidsMap e = case e of
  EncoderPrimitive oids Nothing -> do
    (oid, _) <- resolveOids oidsMap oids
    return (oid, Nothing)
  EncoderPrimitive oids (Just (Right mpoker)) -> do
    (oid, _) <- resolveOids oidsMap oids
    return (oid, Just mpoker)
  EncoderPrimitive oids (Just (Left fields)) -> do
    (oid, _) <- resolveOids oidsMap oids
    fieldsPokers <- traverse (pokeEncoder oidsMap) fields
    let fieldPoker (eoid, Nothing) =
             PE.putInt32BE (unOid eoid)
          <> PE.putInt32BE (-1)
        fieldPoke (eoid, Just (epoke, n)) =
             PE.putInt32BE (unOid eoid)
          <> PE.putWord32BE (fromIntegral n)
          <> epoke
    let poker =
          PE.putWord32BE (fromIntegral . length $ fieldsPokers)
          <> F.foldMap fieldPoker fieldsPokers
    return (oid, Just poker)

-- | Helper that construcst poke encoder for arrays with required buffer size
pokeArray :: OidsMap -> EncoderArray -> Either CompositeName (Oid, Maybe PE.Encode)
pokeArray oidsMap e = case e of
  EncoderArray{..} -> do
    (oidElem, oidArr) <- resolveOids oidsMap encoderArrayElem
    encoders <- fmap snd <$> traverse (pokePrimitive oidsMap) encoderArrayPayload
    let encodeArrayDim ArrayDimension{..} =
             PE.putWord32BE arrayElements
          <> PE.putWord32BE arrayDimStart
    let encoder =
             PE.putWord32BE (fromIntegral . V.length $ encoderArrayDims)
          <> PE.putWord32BE 1
          <> PE.putInt32BE (unOid oidElem)
          <> F.foldMap encodeArrayDim encoderArrayDims
          <> payloadEncoder

        payloadEncoder = flip F.foldMap encoders $ \a -> case a of
          Nothing -> PE.putInt32BE (-1)
          Just e  -> PE.putWord32BE (fromIntegral $ PE.getEncodeLen e) <> e

    return (oidArr, Just encoder)
  EncoderNullArray {..} -> do
    (_, oidArr) <- resolveOids oidsMap encoderNullArrayElem
    return (oidArr, Nothing)

-- | Helper that constructs poke encoder with needed buffer size
pokeEncoder :: OidsMap -> Encoder -> Either CompositeName (Oid, Maybe PE.Encode)
pokeEncoder oidsMap e = case e of
  Encoder'Primitive pe -> pokePrimitive oidsMap pe
  Encoder'Array ae -> pokeArray oidsMap ae

-- | Lookup in composite name to OID map and return 'Left' with the composite name
-- if cannot find the name.
elookup :: CompositeName -> Map CompositeName Oid -> Either CompositeName Oid
elookup name = maybe (Left name) Right . M.lookup name

-- | Helper that resolves given OIDs to 'Right' and left in 'Left' unknown names
resolveOids :: OidsMap -> Either CompositeName (Oid, Oid) -> Either CompositeName (Oid, Oid)
resolveOids _ (Right (e, ea))   = Right (e, ea)
resolveOids oidsMap (Left name) = (,)
  <$> elookup name (oidsPlain oidsMap)
  <*> elookup name (oidsArray oidsMap)

-- | General type class that converts values to PostgreSQL binary format
class ToPg a where
  -- | Transform value to PostgreSQL binary encoder
  toPg :: a -> Encoder

-- | Defines statically known primitive type
class ToPrimitive a where
  -- | Defines primitive OID and OID of array, or defines dynamicly resolved name
  primOids     :: Proxy a -> Either CompositeName (Oid, Oid)
  -- | How to encode the primitive type or composite encoder
  primEncoder  :: a -> Maybe (Either [Encoder] PE.Encode)

-- | Each primitive type has NULL encoder
instance {-# OVERLAPPABLE #-} ToPrimitive a => ToPrimitive (Maybe a) where
  primOids _  = primOids (Proxy :: Proxy a)
  primEncoder = join . fmap primEncoder
  {-# INLINE primOids #-}
  {-# INLINE primEncoder #-}

-- | Embeds PostgreSQL primitive encoder into general Encoder
primitive :: forall a . ToPrimitive a => a -> Encoder
primitive v = Encoder'Primitive EncoderPrimitive {
    encoderPrimOids = primOids (Proxy :: Proxy a)
  , encoderPrimPoke = primEncoder v
  }
{-# INLINE primitive #-}

-- | Defines how to encode Haskell array to PostgreSQL multidimensional array
class ToArray (v :: * -> *) a where
  -- | Return either statically known OIDs of element and array or composite
  -- name of element type.
  arrayElement :: Proxy (v a) -> Either CompositeName (Oid, Oid)
  -- | Defines dimensions of resulting PostgreSQL array
  arrayDimensions :: v a -> Vector ArrayDimension
  -- | Encoder of single element
  arrayEncoder :: Proxy v -> a -> EncoderPrimitive

instance {-# OVERLAPPABLE #-} (ToPrimitive a, Traversable v) => ToArray v a where
  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions as = V.singleton $ ArrayDimension (fromIntegral $ length as) 1
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

-- | Helper that calculates dimensions for PostgreSQL array from Repa shape
repaDims :: R.Shape sh => sh -> Vector ArrayDimension
repaDims sh = V.fromList . flip fmap (R.listOfShape sh) $ \n -> ArrayDimension (fromIntegral n) 1

-- | Multidimentional arrays are neatly expressed by Repa arrays
instance {-# OVERLAPPABLE #-} (ToPrimitive a, R.Shape sh, R.Source r a) => ToArray (R.Array r sh) a where
  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions = repaDims . R.extent
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

-- | Peano naturals, TODO: use library one
data Nat = Zero | Succ Nat

-- | Helpers to calculate metrics of nested arrays
class KnownNestedArray (n :: Nat) (v :: * -> *) a where
  -- | Nested Array type
  type MkNestedArray n v a :: *
  -- | Calculate dimensions of nested array
  nestedSize :: Proxy n -> Proxy v -> Proxy a -> MkNestedArray n v a -> Vector Int
  -- | Make flat vector of nested vector
  nestedFlat :: Proxy n -> Proxy v -> Proxy a -> MkNestedArray n v a -> Vector a

instance Foldable v => KnownNestedArray Zero v a where
  type MkNestedArray Zero v a = v a
  nestedSize _ _ _ = V.singleton . F.length
  nestedFlat _ _ _ = V.fromList . F.toList
  {-# INLINE nestedSize #-}
  {-# INLINE nestedFlat #-}

instance {-# OVERLAPPABLE #-} (Functor v, Foldable v, Traversable v, KnownNestedArray n v a) => KnownNestedArray (Succ n) v a where
  type MkNestedArray (Succ n) v a = v (MkNestedArray n v a)
  nestedSize _ vprox aprox as = maximum (fmap (nestedSize (Proxy :: Proxy n) vprox aprox) as) `V.snoc` F.length as
  nestedFlat _ vrpox aprox as = F.foldl' (V.++) V.empty $ fmap (nestedFlat (Proxy :: Proxy n) vrpox aprox) as
  {-# INLINE nestedSize #-}
  {-# INLINE nestedFlat #-}

-- | Wrapper around nested array. A multidimensional PG array is generated from the newtype.
newtype NestedArray (n :: Nat) (v :: * -> *) a = NestedArray { unNestedArray :: MkNestedArray n v a }

-- | Generate dimension info from nested array
nestedArrayDims :: forall n v a . KnownNestedArray n v a => NestedArray n v a -> Vector ArrayDimension
nestedArrayDims arr = flip ArrayDimension 1 . fromIntegral <$> sizes
  where
    sizes = nestedSize (Proxy :: Proxy n) (Proxy :: Proxy v) (Proxy :: Proxy a) (unNestedArray arr)

-- | Generate flat array from nested array
nestedArrayVector :: forall n v a . KnownNestedArray n v a => NestedArray n v a -> Vector a
nestedArrayVector = nestedFlat (Proxy :: Proxy n) (Proxy :: Proxy v) (Proxy :: Proxy a) . unNestedArray

-- | Generate PG array from nested haskell collections
instance (ToPrimitive a, KnownNestedArray n v a) => ToArray (NestedArray n v) a where
  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions = nestedArrayDims
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

-- | Embeds PostgreSQL array into general encoder
array :: forall v a . (ToArray v a, Traversable v) => v a -> Encoder
array as = Encoder'Array EncoderArray {
    encoderArrayElem = arrayElement (Proxy :: Proxy (v a))
  , encoderArrayDims = arrayDimensions as
  , encoderArrayPayload = V.fromList . F.toList $ arrayEncoder (Proxy :: Proxy v) <$> as
  }

-- | Embeds PostgreSQL array into general encoder
arrayNull :: forall v a . (ToArray v a) => Proxy (v a) -> Encoder
arrayNull p = Encoder'Array EncoderNullArray {
    encoderNullArrayElem = arrayElement p
  }

-- | Embeds PostgreSQL array into general encoder
arrayMay :: forall v a . (ToArray v a, Traversable v) => Maybe (v a) -> Encoder
arrayMay Nothing = arrayNull (Proxy :: Proxy (v a))
arrayMay (Just as) = array as

-- | Embeds PostgreSQL array into general encoder
arrayNested :: forall n v a . (ToArray v a, ToPrimitive a, KnownNestedArray n v a) => NestedArray n v a -> Encoder
arrayNested as = Encoder'Array EncoderArray {
    encoderArrayElem = arrayElement (Proxy :: Proxy (v a))
  , encoderArrayDims = arrayDimensions as
  , encoderArrayPayload = V.fromList . F.toList $ arrayEncoder (Proxy :: Proxy (NestedArray n v)) <$> nestedArrayVector as
  }

-- | Embeds PostgreSQL array into general encoder
arrayNestedMay :: forall n v a . (ToArray v a, ToPrimitive a, KnownNestedArray n v a) => Maybe (NestedArray n v a) -> Encoder
arrayNestedMay Nothing = arrayNull (Proxy :: Proxy (v a))
arrayNestedMay (Just as) = arrayNested as

-- | Convert repa array to PG array with parallelism. Monad is used to ensure that
-- array is computed only once, see 'computeVectorP'.
arrayRepaP :: forall r sh a m . (ToArray (R.Array r sh) a, R.Load r sh a, Monad m) => R.Array r sh a -> m Encoder
arrayRepaP arr = do
  payload <- R.computeVectorP arr
  return $ Encoder'Array EncoderArray {
      encoderArrayElem = arrayElement (Proxy :: Proxy (R.Array r sh a))
    , encoderArrayDims = arrayDimensions arr
    , encoderArrayPayload = fmap (arrayEncoder (Proxy :: Proxy (R.Array r sh))) . R.toVector $ payload
    }

-- | Convert nullable repa array to PG array with parallelism. Monad is used to ensure that
-- array is computed only once, see 'computeVectorP'.
arrayRepaPMay :: forall r sh a m . (ToArray (R.Array r sh) a, R.Load r sh a, Monad m) => Maybe (R.Array r sh a) -> m Encoder
arrayRepaPMay Nothing = pure $ arrayNull (Proxy :: Proxy (R.Array r sh a))
arrayRepaPMay (Just arr) = arrayRepaP arr

-- | Convert repa array to PG array sequentionally.
arrayRepaS :: forall r sh a . (ToArray (R.Array r sh) a, R.Load r sh a) => R.Array r sh a -> Encoder
arrayRepaS arr = Encoder'Array EncoderArray {
    encoderArrayElem = arrayElement (Proxy :: Proxy (R.Array r sh a))
  , encoderArrayDims = arrayDimensions arr
  , encoderArrayPayload = fmap (arrayEncoder (Proxy :: Proxy (R.Array r sh))) . R.toVector . R.computeVectorS $ arr
  }

-- | Convert nullable repa array to PG array with parallelism. Monad is used to ensure that
-- array is computed only once, see 'computeVectorP'.
arrayRepaSMay :: forall r sh a . (ToArray (R.Array r sh) a, R.Load r sh a) => Maybe (R.Array r sh a) -> Encoder
arrayRepaSMay Nothing = arrayNull (Proxy :: Proxy (R.Array r sh a))
arrayRepaSMay (Just arr) = arrayRepaS arr

testVector :: Encoder
testVector = array $ V.fromList [1, 2, 3, 4 :: Word16]

vec2 :: NestedArray (Succ Zero) Vector Word16
vec2 = NestedArray $ V.fromList [V.fromList [1, 2], V.fromList [3, 4]]

vec3 :: NestedArray (Succ (Succ Zero)) Vector Word16
vec3 = NestedArray $ V.fromList [
    V.fromList [
      V.fromList [1, 2]
    , V.fromList [3, 4]
    ]
  , V.fromList [
      V.fromList [5, 6]
    , V.fromList [7, 8]
    ]
  ]

testVector2 :: Encoder
testVector2 = arrayNested vec2

testVector3 :: Encoder
testVector3 = arrayNested vec2

testRepa1 :: Encoder
testRepa1 = arrayRepaS $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

testRepa2 :: Encoder
testRepa2 = runIdentity . arrayRepaP $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

-- | smallint
instance ToPrimitive Word16 where
  primOids _  = Right (Oid 21, Oid 1005)
  primEncoder = Just . Right . PE.putWord16BE
  {-# INLINE primOids #-}
  {-# INLINE primEncoder #-}


-- | integer
instance ToPg Word32 where
  toPg = undefined
  {-# INLINE toPg #-}

-- | bigint
instance ToPg Word64 where
  toPg = undefined
  {-# INLINE toPg #-}

-- | real
instance ToPg Float where
  toPg = undefined
  {-# INLINE toPg #-}

-- | double precision
instance ToPg Double where
  toPg = undefined
  {-# INLINE toPg #-}

-- | text, varchar
instance ToPg String where
  toPg = undefined
  {-# INLINE toPg #-}

-- | text, varchar
instance ToPg T.Text where
  toPg = undefined
  {-# INLINE toPg #-}

-- | text, varchar
instance ToPg TL.Text where
  toPg = undefined
  {-# INLINE toPg #-}

-- | bytea
instance ToPg BS.ByteString where
  toPg = undefined
  {-# INLINE toPg #-}

-- | bool
instance ToPg Bool where
  toPg = undefined
  {-# INLINE toPg #-}

-- | oid
instance ToPg Oid where
  toPg = undefined
  {-# INLINE toPg #-}
