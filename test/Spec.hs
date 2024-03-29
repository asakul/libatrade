
import qualified TestBrokerClient
import qualified TestBrokerProtocol
import qualified TestBrokerServer
import qualified TestQuoteSourceClient
import qualified TestQuoteSourceServer
import qualified TestTypes
import qualified TestZMQTradeSink

import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Tests" [properties, unitTests]

properties :: TestTree
properties = testGroup "Properties" [TestTypes.properties, TestBrokerProtocol.properties]

unitTests :: TestTree
unitTests = testGroup "Unit-tests" [
    TestQuoteSourceClient.unitTests
  , TestQuoteSourceServer.unitTests
  , TestBrokerServer.unitTests
  , TestBrokerClient.unitTests
  , TestZMQTradeSink.unitTests ]

