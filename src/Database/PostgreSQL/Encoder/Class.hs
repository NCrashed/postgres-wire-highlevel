module Database.PostgreSQL.Encoder.Class(
    Encoder(..)
  , EncoderPrimitive(..)
  , EncoderArray(..)
  , ArrayDimension(..)
  , OidsMap(..)
  , runEncoder
  , ToPg(..)
  , ToPrimitive(..)
  , primitive
  , ToArray(..)
  ) where

import Control.Monad
import Data.Kind
import Data.Map (Map)
import Data.Monoid
import Data.Proxy
import Data.Vector (Vector)
import Data.Word
import Database.PostgreSQL.Protocol.Types
import qualified Data.ByteString as BS
import qualified Data.Foldable as F
import qualified Data.Map.Strict as M
import qualified Data.Text as T
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
        fieldPoker (eoid, Just epoke) =
             PE.putInt32BE (unOid eoid)
          <> PE.putWord32BE (fromIntegral $ PE.getEncodeLen epoke)
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
          Just pe -> PE.putWord32BE (fromIntegral $ PE.getEncodeLen pe) <> pe

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
  -- | Type specific constraint on array
  type ToPgArrayCond v a :: Constraint

  -- | Return either statically known OIDs of element and array or composite
  -- name of element type.
  arrayElement :: Proxy (v a) -> Either CompositeName (Oid, Oid)
  -- | Defines dimensions of resulting PostgreSQL array
  arrayDimensions :: v a -> Vector ArrayDimension
  -- | Encoder of single element
  arrayEncoder :: Proxy v -> a -> EncoderPrimitive

  -- | Embeds PostgreSQL array into general encoder
  array :: (ToArray v a, ToPgArrayCond v a) => v a -> Encoder

  -- | Embeds NULL PostgreSQL array into general encoder
  arrayNull :: (ToArray v a, ToPgArrayCond v a) => Proxy (v a) -> Encoder
  arrayNull p = Encoder'Array EncoderNullArray {
      encoderNullArrayElem = arrayElement p
    }

  -- | Embeds nullable PostgreSQL array into general encoder
  arrayMay :: (ToArray v a, ToPgArrayCond v a) => Maybe (v a) -> Encoder
  arrayMay Nothing = arrayNull (Proxy :: Proxy (v a))
  arrayMay (Just as) = array as
