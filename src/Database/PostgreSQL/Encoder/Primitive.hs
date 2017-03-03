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

instance ToPrimitive TestEnum where
  primOids _ = dynamicOid "TestEnum"
  primEncoder = enum

data TestData = TestData {
  field1 :: Word16
, field2 :: Word32
, field3 :: [Word16]
, field4 :: Vector Word32
, field5 :: TestEnum
, field6 :: Vector (Vector TestEnum)
, field7 :: Maybe TestData
}

instance ToPrimitive TestData where
  primOids _ = dynamicOid "TestData"
  primEncoder TestData{..} = composite [
      toPg field1
    , toPg field2
    , toPg field3
    , toPg field4
    , toPg field5
    , array (NestedArray field6 :: NestedArray ('Succ 'Zero) Vector TestEnum)
    , toPg field7
    ]


testNullArray1 :: Encoder
testNullArray1 = arrayNull (Proxy :: Proxy (FlatArray Vector Word16))

testNullArray2 :: Encoder
testNullArray2 = arrayNull (Proxy :: Proxy (NestedArray ('Succ 'Zero) Vector Word16))

testVector :: Encoder
testVector = toPg $ V.fromList [1, 2, 3, 4 :: Word16]

vec2 :: NestedArray ('Succ 'Zero) Vector Word16
vec2 = NestedArray $ V.fromList [V.fromList [1, 2], V.fromList [3, 4]]

vec3 :: NestedArray ('Succ ('Succ 'Zero)) Vector Word16
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
testVector2 = array vec2

testVector3 :: Encoder
testVector3 = array vec2

testRepa1 :: Encoder
testRepa1 = array $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

testRepa2 :: Encoder
testRepa2 = runIdentity . arrayRepaP $ R.fromFunction (R.ix2 2 2) $ const (0 :: Word16)

-- | smallint
instance ToPrimitive Word16 where
  primOids _  = staticOid (Oid 21) (Oid 1005)
  primEncoder = prim . PE.putWord16BE
  {-# INLINE primOids #-}
  {-# INLINE primEncoder #-}

-- | integer
instance ToPrimitive Word32 where
  primOids _  = staticOid (Oid 23) (Oid 1007)
  primEncoder = prim . PE.putWord32BE
  {-# INLINE primOids #-}
  {-# INLINE primEncoder #-}

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
