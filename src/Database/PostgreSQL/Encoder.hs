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
import Data.Int
import Data.Map (Map)
import Data.Monoid
import Data.Proxy
import Data.Store (poke)
import Data.Store.Core
import Data.Vector (Vector)
import Data.Word
import Database.PostgreSQL.Protocol.Types
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
    -- | Primitive encoder with known oid, oid of array and size.
      Encoder'Primitive !EncoderPrimitive
    -- | Encoder of array of given element
    | Encoder'Array !EncoderArray
    -- | Encoder of composites with statically known names
    | Encoder'Composite !EncoderComposite

-- QUESTION: Is composite actually a primitive subtype?

-- | Holds info how to serialise primitive PostgreSQL value
data EncoderPrimitive = EncoderPrimitive {
    -- | OID of type
    encoderPrimOid    :: !Oid
    -- | OID of array of the type
  , encoderPrimArrOid :: !Oid
    -- | Either a Null 'Nothing' or poker with size of required buffer
  , encoderPrimPoke   :: !(Maybe PE.Encode)
  }
  -- | Holds info how to serialise user defined primitive types for PostgreSQL
  | EncoderBase {
    -- | Name of base type
    encoderBaseName   :: !CompositeName
    -- | Either a Null 'Nothing' or poker with size of required buffer
  , encoderBasePoke   :: !(Maybe PE.Encode)
  }

-- | Holds info how to serialise PostgreSQL arrays
data EncoderArray = EncoderArray {
    -- | Array oids info, either known element and array type or composite type
      encoderArrayElem    :: !(Either CompositeName (Oid, Oid))
    -- | Dimensions of array
    , encoderArrayDims    :: !(Vector ArrayDimension)
    -- | Poker for contents of the array with size of required buffer
    --
    -- Important note: that each element of the payload should be length tagged.
    -- First goes word32 length in bytes of element contents, next the rest
    -- payload of element is continued.
    , encoderArrayPayload :: !PE.Encode
    }
  -- | Special case for NULL array
  | EncoderNullArray {
      -- | Encoder for element (we need oids or name from the field)
      encoderNullArrayElem  :: !(Either CompositeName (Oid, Oid))
      -- | Number of dimensions
    , encoderNullArrayDims  :: !Word32
    }

-- | Specification of array dimension
data ArrayDimension = ArrayDimension {
  -- | Size of dimension
    arrayElements :: Word32
  -- | Start index of the dimension. Almost always 1.
  , arrayDimStart :: Word32
  }

-- | Holds info how to serialise PostgreSQL composite type
data EncoderComposite = EncoderComposite {
    -- | Statically known name of composite type
    encoderCompositeName   :: !CompositeName
    -- | If 'Nothing' the composite is NULL, else holds encoded fields
  , encoderCompositeFields :: !(Maybe [Encoder])
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

-- | Helper that constructs poke encoder with needed buffer size
pokeEncoder :: OidsMap -> Encoder -> Either CompositeName (Oid, Maybe PE.Encode)
pokeEncoder oidsMap e = case e of
  Encoder'Primitive (EncoderPrimitive oid _ mpoker) -> Right (oid, mpoker)
  Encoder'Primitive (EncoderBase name mpoker) -> (, mpoker) <$> elookup name (oidsPlain oidsMap)
  Encoder'Array EncoderArray{..} -> do
    (oidElem, oidArr) <- resolveOids oidsMap encoderArrayElem
    let encodeArrayDim ArrayDimension{..} =
             PE.putWord32BE arrayElements
          <> PE.putWord32BE arrayDimStart
    let encoder =
             PE.putWord32BE (fromIntegral . V.length $ encoderArrayDims)
          <> PE.putWord32BE 1
          <> PE.putInt32BE (unOid oidElem)
          <> F.foldMap encodeArrayDim encoderArrayDims
          <> encoderArrayPayload
    return (oidArr, Just encoder)
  Encoder'Array EncoderNullArray {..} -> do
    (_, oidArr) <- resolveOids oidsMap encoderNullArrayElem
    let poker =
             PE.putWord32BE encoderNullArrayDims
          <> PE.putWord32BE 0
    return (oidArr, Just poker)
  Encoder'Composite EncoderComposite {..} -> do
    oid <- elookup encoderCompositeName $ oidsPlain oidsMap
    case encoderCompositeFields of
      Nothing -> return (oid, Nothing)
      Just fields -> do
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
  -- | How to encode the primitive type
  primEncoder  :: a -> Maybe PE.Encode

-- | Each primitive type has NULL encoder
instance {-# OVERLAPPABLE #-} ToPrimitive a => ToPrimitive (Maybe a) where
  primOids _  = primOids (Proxy :: Proxy a)
  primEncoder = join . fmap primEncoder
  {-# INLINE primOids #-}
  {-# INLINE primEncoder #-}

-- | Embeds PostgreSQL primitive encoder into general Encoder
primitive :: forall a . ToPrimitive a => a -> Encoder
primitive v = Encoder'Primitive $ case primOids (Proxy :: Proxy a) of
  Left name -> EncoderBase {
      encoderBaseName = name
    , encoderBasePoke = primEncoder v
    }
  Right (oid, oidArr) -> EncoderPrimitive {
      encoderPrimOid    = oid
    , encoderPrimArrOid = oidArr
    , encoderPrimPoke   = primEncoder v
    }
{-# INLINE primitive #-}

-- | Defines how to encode Haskell array to PostgreSQL multidimensional array
class ToArray (v :: * -> *) a where
  -- | Return either statically known OIDs of element and array or composite
  -- name of element type.
  arrayElement :: Proxy (v a) -> Either CompositeName (Oid, Oid)
  -- | Statically known number of dimensions
  arrayDimensionsCount :: Proxy (v a) -> Word32
  -- | Defines dimensions of resulting PostgreSQL array
  arrayDimensions :: v a -> Vector ArrayDimension
  -- | Encoder of single element
  arrayEncoder :: Proxy v -> a -> Maybe PE.Encode

instance {-# OVERLAPPABLE #-} ToPrimitive a => ToArray [] a where
  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensionsCount _ = 1
  arrayDimensions as = V.singleton $ ArrayDimension (fromIntegral $ length as) 1
  arrayEncoder _ = primEncoder
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensionsCount #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

-- TODO: Composite support as element
-- TODO: Multidimentional example

-- | Helper that collects payload for array elements
arrayPayloadEncoder :: forall v a . (ToArray v a, Foldable v) => v a -> PE.Encode
arrayPayloadEncoder = F.foldMap $ \a -> case arrayEncoder (Proxy :: Proxy v) a of
  Nothing -> PE.putInt32BE (-1)
  Just e  -> PE.putWord32BE (fromIntegral $ PE.getEncodeLen e) <> e

-- | Embeds PostgreSQL array into general encoder
array :: forall v a . (ToArray v a, Foldable v) => v a -> Encoder
array as = Encoder'Array EncoderArray {
    encoderArrayElem = arrayElement (Proxy :: Proxy (v a))
  , encoderArrayDims = arrayDimensions as
  , encoderArrayPayload = arrayPayloadEncoder as
  }

-- | Embeds PostgreSQL array into general encoder
arrayNull :: forall v a . (ToArray v a) => Proxy (v a) -> Encoder
arrayNull p = Encoder'Array EncoderNullArray {
    encoderNullArrayElem = arrayElement p
  , encoderNullArrayDims = arrayDimensionsCount p
  }

-- | Embeds PostgreSQL array into general encoder
arrayMay :: forall v a . (ToArray v a, Foldable v) => Maybe (v a) -> Encoder
arrayMay Nothing = arrayNull (Proxy :: Proxy (v a))
arrayMay (Just as) = array as

-- | smallint
instance ToPrimitive Word16 where
  primOids _  = Right (Oid 21, Oid 1005)
  primEncoder = Just . PE.putWord16BE
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
