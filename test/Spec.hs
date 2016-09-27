
import qualified TestTypes
import qualified TestQuoteSourceServer

import Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [properties, unitTests]

properties :: TestTree
properties = testGroup "Properties" [TestTypes.properties]

unitTests :: TestTree
unitTests = testGroup "Unit-tests" [TestQuoteSourceServer.unitTests]

