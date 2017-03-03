module Database.PostgreSQL.Encoder.Class(
    ArrayDimension(..)
  , OidsMap(..)
  , Encoder(..)
  , OidInfo(..)
  , runEncoder
  , nullEncoder
  , primitive
  , composite
  , enum
  , array
  , elements
  , ToPg(..)
  , IsValidArray
  ) where

import Control.Monad
import Data.Kind
import Data.Map (Map)
import Data.Monoid
import Data.Proxy
import Data.Vector (Vector)
import Data.Word
import Database.PostgreSQL.Protocol.Types
import GHC.Generics
import qualified Data.ByteString as BS
import qualified Data.Foldable as F
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Database.PostgreSQL.Protocol.Store.Encode as PE

-- | Name of composite type in postgres. We need the info
type CompositeName = T.Text

-- | Strict tuple for result of encoder, last element is not forced to allow
-- getting oids from 'undefined' input.
data EncoderResult = ER !Oid !Oid (Maybe PE.Encode)

-- | Wrapped action of encoding a Haskell value to PostgreSQL binary format
newtype Encoder = Encoder { unEncoder :: OidsMap -> Either CompositeName EncoderResult }

-- | Speical map that stores mapping from type name to oid
data OidsMap = OidsMap {
  -- | Map that stores oids for composite types
  oidsPlain :: !(Map CompositeName Oid)
  -- | Map that stores oids for array types for given name of composite type
, oidsArray :: !(Map CompositeName Oid)
} deriving (Generic)

-- | Specification of array dimension
data ArrayDimension = ArrayDimension {
-- | Size of dimension
  arrayElements :: !Word32
-- | Start index of the dimension. Almost always 1.
, arrayDimStart :: !Word32
} deriving (Generic)

-- | Oid info is either a dynamically resolved composite name or known pair of
-- the oid of plain type and the oid of array type.
data OidInfo = OidComposite !CompositeName | OidKnown !Oid !Oid
  deriving (Eq, Show)

-- | Execute encoder and get final pair of oid of field and binary representation
runEncoder :: OidsMap -> Encoder -> Either CompositeName (Oid, Maybe BS.ByteString)
runEncoder m e = (\(ER oid _ ma) -> (oid, fmap PE.runEncode ma)) <$> unEncoder e m
{-# INLINE runEncoder #-}

-- | General type class that converts values to PostgreSQL binary format
class ToPg a where
  -- | Transform any value to PostgreSQL binary encoder
  toPg :: a -> Encoder

  -- | Get oid info of the type, for arrays, should return oid of element
  getOid :: Proxy a -> OidInfo

  -- | Tag that 'a' is a PG array
  type IsArray a :: Bool
  type IsArray a = 'False

  -- | Tag that 'a' is a nullable PG type
  type IsNullable a :: Bool
  type IsNullable a = 'False

  -- | Extract dimension info from array
  arrayDimensions :: a -> Vector ArrayDimension
  arrayDimensions = const V.empty

-- | Make NULL value encoder
nullEncoder :: OidInfo -> Encoder
nullEncoder oinfo = Encoder $ \m -> do
  (oid, oidArr) <- resolveOids m oinfo
  pure $ ER oid oidArr Nothing
{-# INLINE nullEncoder #-}

-- | Make encoder of primitive type
primitive :: OidInfo -> PE.Encode -> Encoder
primitive oinfo e = Encoder $ \m -> do
  (oid, oidArr) <- resolveOids m oinfo
  pure $ ER oid oidArr (Just e)
{-# INLINE primitive #-}

-- | Make encoder from enum value
enum :: Show a => OidInfo -> a -> Encoder
enum oinfo a = primitive oinfo e
  where
    ae = PE.putPgString . T.encodeUtf8 . T.pack . show $ a
    e = PE.putWord32BE (fromIntegral . PE.getEncodeLen $ ae) <> ae
{-# INLINE enum #-}

-- | Make encoder of composite type
composite :: Traversable f => OidInfo -> f Encoder -> Encoder
composite oinfo fields = Encoder $ \m -> do
  (oid, oidArr) <- resolveOids m oinfo
  fieldsPokers <- traverse (($ m) . unEncoder) fields
  let fieldPoker (ER eoid _ Nothing) =
           PE.putInt32BE (unOid eoid)
        <> PE.putInt32BE (-1)
      fieldPoker (ER eoid _ (Just epoke)) =
           PE.putInt32BE (unOid eoid)
        <> PE.putWord32BE (fromIntegral $ PE.getEncodeLen epoke)
        <> epoke
  let poker =
        PE.putWord32BE (fromIntegral . length $ fieldsPokers)
        <> F.foldMap fieldPoker fieldsPokers
  return $ ER oid oidArr (Just poker)
{-# INLINE composite #-}

-- | Make encoder for array type, use 'element' helper to encode
-- a single element of array.
array :: Foldable f => OidInfo -> f ArrayDimension -> (OidsMap -> Either CompositeName PE.Encode) -> Encoder
array oinfo dims payloadEnc = Encoder $ \m -> do
  (oidElem, oidArr) <- resolveOids m oinfo
  let encodeArrayDim ArrayDimension{..} =
           PE.putWord32BE arrayElements
        <> PE.putWord32BE arrayDimStart
  payload <- payloadEnc m
  let encoder =
           PE.putWord32BE (fromIntegral . F.length $ dims)
        <> PE.putWord32BE 1
        <> PE.putInt32BE (unOid oidElem)
        <> F.foldMap encodeArrayDim dims
        <> payload
  return $ ER oidArr oidArr (Just encoder)
{-# INLINE array #-}

-- | Wrap array element, intented to use with 'array' helper
elements :: forall a f . (Functor f, Foldable f, Traversable f) => ToPg a => f a -> OidsMap -> Either CompositeName PE.Encode
elements es m = do
  let
    element e = case unEncoder e m of
      Right (ER _ _ Nothing) -> pure $ PE.putInt32BE (-1)
      Right (ER _ _ (Just pe)) -> pure $
        PE.putWord32BE (fromIntegral $ PE.getEncodeLen pe) <> pe
      Left er -> Left er
  body <- traverse (element . toPg) es
  pure $ F.foldMap id body
{-# INLINE elements #-}

-- | Lookup in composite name to OID map and return 'Left' with the composite name
-- if cannot find the name.
elookup :: CompositeName -> Map CompositeName Oid -> Either CompositeName Oid
elookup name = maybe (Left name) Right . M.lookup name
{-# INLINE elookup #-}

-- | Helper that resolves given OIDs to 'Right' and left in 'Left' unknown names
resolveOids :: OidsMap -> OidInfo -> Either CompositeName (Oid, Oid)
resolveOids _ (OidKnown e ea)   = Right (e, ea)
resolveOids oidsMap (OidComposite name) = (,)
  <$> elookup name (oidsPlain oidsMap)
  <*> elookup name (oidsArray oidsMap)
{-# INLINE resolveOids #-}

instance ToPg a => ToPg (Maybe a) where
  type IsNullable (Maybe a) = 'True
  type IsArray (Maybe a) = IsArray a

  toPg Nothing = nullEncoder (getOid (Proxy :: Proxy a))
  toPg (Just v) = toPg v
  {-# INLINE toPg #-}

  getOid _ = getOid (Proxy :: Proxy a)
  {-# INLINE getOid #-}

-- | Restrics set of types that can be used as subarrays
type family IsValidArray (isArray :: Bool) (isNullable :: Bool) where
  IsValidArray 'True 'True = 'False
  IsValidArray _ _ = 'True
