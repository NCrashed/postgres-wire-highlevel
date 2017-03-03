{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.PostgreSQL.Encoder.Primitive(

  ) where

import Data.Functor.Identity
import Data.Proxy
import Data.Vector (Vector)
import Data.Word
import Database.PostgreSQL.Protocol.Types
import qualified Data.Array.Repa as R
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
import qualified Database.PostgreSQL.Protocol.Store.Encode as PE

import Database.PostgreSQL.Encoder.Class
import Database.PostgreSQL.Encoder.Array

data TestEnum = Option1 | Option2 | Option3
  deriving (Read, Show, Eq, Ord, Enum, Bounded)

instance ToPg TestEnum where
  toPg = enum $ getOid (Proxy :: Proxy TestEnum)
  getOid _ = OidComposite "TestEnum"

data TestData = TestData {
  field1 :: Word16
, field2 :: Word32
, field3 :: [Word16]
, field4 :: Vector Word32
, field5 :: TestEnum
, field6 :: Vector (Vector TestEnum)
, field7 :: Maybe TestData
}

instance ToPg TestData where
  toPg TestData{..} = composite (getOid (Proxy :: Proxy TestData)) [
      toPg field1
    , toPg field2
    , toPg field3
    , toPg field4
    , toPg field5
    , toPg field6
    , toPg field7
    ]
  getOid _ = OidComposite "TestData"

testNullArray1 :: Encoder
testNullArray1 = nullEncoder $ getOid (Proxy :: Proxy (Vector Word16))

testNullArray2 :: Encoder
testNullArray2 = nullEncoder $ getOid (Proxy :: Proxy (Vector (Vector Word16)))

testVector :: Encoder
testVector = toPg $ V.fromList [1, 2, 3, 4 :: Word16]

vec2 :: Vector (Vector Word16)
vec2 = V.fromList [V.fromList [1, 2], V.fromList [3, 4]]

vec3 :: Vector (Vector (Vector Word16))
vec3 = V.fromList [
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
testVector2 = toPg vec2

testVector3 :: Encoder
testVector3 = toPg vec2

testRepa1 :: Encoder
testRepa1 = toPg $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

testRepa2 :: Encoder
testRepa2 = runIdentity . arrayRepaP $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

-- | smallint
instance ToPg Word16 where
  getOid _  = OidKnown (Oid 21) (Oid 1005)
  toPg = primitive (getOid (Proxy :: Proxy Word16)) . PE.putWord16BE
  {-# INLINE getOid #-}
  {-# INLINE toPg #-}

-- | integer
instance ToPg Word32 where
  getOid _  = OidKnown (Oid 23) (Oid 1007)
  toPg = primitive (getOid (Proxy :: Proxy Word32)) . PE.putWord32BE
  {-# INLINE getOid #-}
  {-# INLINE toPg #-}

-- | bigint
-- instance ToPg Word64 where
--   getOid _  = OidKnown (Oid 20) (Oid 1016)
--   toPg = primitive (getOid (Proxy :: Proxy Word64)) . PE.putWord64BE
--   {-# INLINE getOid #-}
--   {-# INLINE toPg #-}

-- | real
instance ToPg Float where
  toPg = undefined
  {-# INLINE toPg #-}

-- | double precision
instance ToPg Double where
  toPg = undefined
  {-# INLINE toPg #-}

-- | text, varchar
-- instance ToPg String where
--   toPg = undefined
--   {-# INLINE toPg #-}

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
