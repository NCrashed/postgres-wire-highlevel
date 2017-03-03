{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.PostgreSQL.Encoder.Array(
    arrayRepaP
  , arrayRepaPMay
  ) where

import Data.Proxy
import Data.Vector (Vector)
import qualified Data.Array.Repa as R
import qualified Data.Array.Repa.Eval as R
import qualified Data.Array.Repa.Repr.Vector as R
import qualified Data.Foldable as F
import qualified Data.Sequence as S
import qualified Data.Vector as V

import Database.PostgreSQL.Encoder.Class


--------------------------------------------------------------------------------
-- Repa array
--------------------------------------------------------------------------------

-- | Helper that calculates dimensions for PostgreSQL array from Repa shape
repaDims :: R.Shape sh => sh -> Vector ArrayDimension
repaDims sh = V.fromList . flip fmap (R.listOfShape sh) $ \n -> ArrayDimension (fromIntegral n) 1

instance {-# OVERLAPPABLE #-} (ToPg a, IsArray a ~ 'False, R.Shape sh, R.Source r a, R.Load r sh a) => ToPg (R.Array r sh a) where
  type IsArray (R.Array r sh a) = 'True

  toPg v = array (getOid (Proxy :: Proxy a)) (arrayDimensions v) $ elements . R.toVector . R.computeVectorS $ v
  {-# INLINE toPg #-}

  getOid _ = getOid (Proxy :: Proxy a)
  {-# INLINE getOid #-}

  arrayDimensions = repaDims . R.extent
  {-# INLINE arrayDimensions #-}

-- | Convert repa array to PG array with parallelism. Monad is used to ensure that
-- array is computed only once, see 'computeVectorP'.
arrayRepaP :: forall r sh a m . (ToPg a, ToPg (R.Array r sh a), R.Load r sh a, Monad m) => R.Array r sh a -> m Encoder
arrayRepaP v = do
  payload <- R.computeVectorP v
  return $ array (getOid (Proxy :: Proxy a)) (arrayDimensions v) $ elements . R.toVector . R.computeVectorS $ v

-- | Convert nullable repa array to PG array with parallelism. Monad is used to ensure that
-- array is computed only once, see 'computeVectorP'.
arrayRepaPMay :: forall r sh a m . (ToPg a, ToPg (R.Array r sh a), R.Load r sh a, Monad m) => Maybe (R.Array r sh a) -> m Encoder
arrayRepaPMay Nothing = pure $ nullEncoder $ getOid (Proxy :: Proxy (R.Array r sh a))
arrayRepaPMay (Just arr) = arrayRepaP arr

instance {-# OVERLAPPABLE #-} (ToPg a, IsValidArray (IsArray a) (IsNullable a) ~ 'True) => ToPg (V.Vector a) where
  type IsArray (V.Vector a) = 'True

  toPg v = array (getOid (Proxy :: Proxy a)) (arrayDimensions v) $ elements v
  {-# INLINE toPg #-}

  getOid _ = getOid (Proxy :: Proxy a)
  {-# INLINE getOid #-}

  arrayDimensions v = subdims `V.snoc` dim
    where
      dim = ArrayDimension (fromIntegral $ length v) 1
      subdims = if V.null v then V.empty else arrayDimensions (V.head v)
  {-# INLINE arrayDimensions #-}

instance {-# OVERLAPPABLE #-} (ToPg a, IsValidArray (IsArray a) (IsNullable a) ~ 'True) => ToPg [a] where
  type IsArray [a] = 'True

  toPg v = array (getOid (Proxy :: Proxy a)) (arrayDimensions v) $ elements v
  {-# INLINE toPg #-}

  getOid _ = getOid (Proxy :: Proxy a)
  {-# INLINE getOid #-}

  arrayDimensions v = subdims `V.snoc` dim
    where
      dim = ArrayDimension (fromIntegral $ length v) 1
      subdims = case v of
        [] -> V.empty
        (v' : _) -> arrayDimensions v'
  {-# INLINE arrayDimensions #-}

instance {-# OVERLAPPABLE #-} (ToPg a, IsValidArray (IsArray a) (IsNullable a) ~ 'True) => ToPg (S.Seq a) where
  type IsArray (S.Seq a) = 'True

  toPg v = array (getOid (Proxy :: Proxy a)) (arrayDimensions v) $ elements v
  {-# INLINE toPg #-}

  getOid _ = getOid (Proxy :: Proxy a)
  {-# INLINE getOid #-}

  arrayDimensions v = subdims `V.snoc` dim
    where
      dim = ArrayDimension (fromIntegral $ length v) 1
      subdims = case S.viewl v of
        S.EmptyL -> V.empty
        (v' S.:< _) -> arrayDimensions v'
  {-# INLINE arrayDimensions #-}
