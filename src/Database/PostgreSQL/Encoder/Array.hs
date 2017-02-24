{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.PostgreSQL.Encoder.Array(
    FlatArray(..)
  , Nat(..)
  , NestedArray(..)
  , arrayRepaP
  , arrayRepaPMay
  ) where

import Data.Proxy
import Data.Vector (Vector)
import qualified Data.Array.Repa as R
import qualified Data.Array.Repa.Eval as R
import qualified Data.Array.Repa.Repr.Vector as R
import qualified Data.Foldable as F
import qualified Data.Vector as V

import Database.PostgreSQL.Encoder.Class

--------------------------------------------------------------------------------
-- Flat array
--------------------------------------------------------------------------------

newtype FlatArray v a = FlatArray { unFlatArray :: v a }
  deriving (Functor, Foldable, Traversable, Show, Eq, Ord)

instance {-# OVERLAPPABLE #-} (ToPrimitive a, Traversable v) => ToArray (FlatArray v) a where
  type ToPgArrayCond (FlatArray v) a = (ToPrimitive a, Traversable v, Foldable v)

  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions as = V.singleton $ ArrayDimension (fromIntegral $ length as) 1
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

  array as = Encoder'Array EncoderArray {
      encoderArrayElem = arrayElement (Proxy :: Proxy (FlatArray v a))
    , encoderArrayDims = arrayDimensions as
    , encoderArrayPayload = V.fromList . F.toList $ arrayEncoder (Proxy :: Proxy (FlatArray v)) <$> as
    }

--------------------------------------------------------------------------------
-- Repa array
--------------------------------------------------------------------------------

-- | Helper that calculates dimensions for PostgreSQL array from Repa shape
repaDims :: R.Shape sh => sh -> Vector ArrayDimension
repaDims sh = V.fromList . flip fmap (R.listOfShape sh) $ \n -> ArrayDimension (fromIntegral n) 1

-- | Multidimentional arrays are neatly expressed by Repa arrays
instance {-# OVERLAPPABLE #-} (ToPrimitive a, R.Shape sh, R.Source r a) => ToArray (R.Array r sh) a where
  type ToPgArrayCond (R.Array r sh) a = (ToPrimitive a, R.Load r sh a)

  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions = repaDims . R.extent
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

  array arr = Encoder'Array EncoderArray {
      encoderArrayElem = arrayElement (Proxy :: Proxy (R.Array r sh a))
    , encoderArrayDims = arrayDimensions arr
    , encoderArrayPayload = fmap (arrayEncoder (Proxy :: Proxy (R.Array r sh))) . R.toVector . R.computeVectorS $ arr
    }

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
arrayRepaPMay :: forall r sh a m . (ToArray (R.Array r sh) a, R.Load r sh a, Monad m, ToPgArrayCond (R.Array r sh) a) => Maybe (R.Array r sh a) -> m Encoder
arrayRepaPMay Nothing = pure $ arrayNull (Proxy :: Proxy (R.Array r sh a))
arrayRepaPMay (Just arr) = arrayRepaP arr

--------------------------------------------------------------------------------
-- Nested Array
--------------------------------------------------------------------------------

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

instance Foldable v => KnownNestedArray 'Zero v a where
  type MkNestedArray 'Zero v a = v a
  nestedSize _ _ _ = V.singleton . F.length
  nestedFlat _ _ _ = V.fromList . F.toList
  {-# INLINE nestedSize #-}
  {-# INLINE nestedFlat #-}

instance {-# OVERLAPPABLE #-} (Functor v, Foldable v, Traversable v, KnownNestedArray n v a) => KnownNestedArray ('Succ n) v a where
  type MkNestedArray ('Succ n) v a = v (MkNestedArray n v a)
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
  type ToPgArrayCond (NestedArray n v) a = (ToPrimitive a, KnownNestedArray n v a, Traversable v)

  arrayElement _ = primOids (Proxy :: Proxy a)
  arrayDimensions = nestedArrayDims
  arrayEncoder _ v = EncoderPrimitive {
      encoderPrimOids = primOids (Proxy :: Proxy a)
    , encoderPrimPoke = primEncoder v
    }
  {-# INLINE arrayElement #-}
  {-# INLINE arrayDimensions #-}
  {-# INLINE arrayEncoder #-}

  array as = Encoder'Array EncoderArray {
      encoderArrayElem = arrayElement (Proxy :: Proxy (NestedArray n v a))
    , encoderArrayDims = arrayDimensions as
    , encoderArrayPayload = V.fromList . F.toList $ arrayEncoder (Proxy :: Proxy (NestedArray n v)) <$> nestedArrayVector as
    }
