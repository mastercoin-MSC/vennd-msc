/**
 * Created by whoisjeremylam on 20/03/14.
 */

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')

//@Grapes([
//@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2'),
//@GrabConfig(systemClassLoader=true)
//])

import org.apache.log4j.*
import groovy.sql.Sql

class MastercoinFollower {
    static logger
    static log4j
    static mastercoinAPI
    static bitcoinAPI
    static satoshi = 100000000
    static int inceptionBlock
    static assetConfig
    static boolean testMode
    static int confirmationsRequired
    static int sleepIntervalms
    static String databaseName

    static db
	
    public class Payment {
        def String inAsset
        def Long currentBlock
        def String txid
        def String sourceAddress
        def String destinationAddress
        def String outAsset
        def Long outAmount
        def Long lastModifiedBlockId
        def String status
		def String outAssetType

        public Payment(String inAssetValue, Long currentBlockValue, String txidValue, String sourceAddressValue, String destinationAddressValue, String outAssetValue, String outAssetTypeValue, Long outAmountValue, Long lastModifiedBlockIdValue, Long originalAmount) {
            def row
            inAsset = inAssetValue
            outAsset = outAssetValue
            outAmount = outAmountValue
            currentBlock = currentBlockValue
			outAssetType = outAssetTypeValue
            txid = txidValue
            lastModifiedBlockId = lastModifiedBlockIdValue
            def assetConfigIndex = assetConfig.findIndexOf { a ->
                a.mastercoinAssetName == inAssetValue
            }		

            if (originalAmount <= 10000000) {
                status = 'authorized'
            }
            else {
                status = 'valid'
            }

			// REMOVED SUPPORT FOR API ADDRESSES			
			sourceAddress = destinationAddressValue
			destinationAddress = sourceAddressValue        
        }
    }

    public init() {

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("MastercoinFollower_log4j.properties")
        log4j = logger.getRootLogger()

		mastercoinAPI = new MastercoinAPI(log4j)
        bitcoinAPI = new BitcoinAPI()
		
        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("MastercoinFollower.ini").toURL())
        inceptionBlock = iniConfig.inceptionBlock
        testMode = iniConfig.testMode
        sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        confirmationsRequired = iniConfig.confirmationsRequired
		
		assetConfig = Asset.readAssets("AssetInformation.ini")

        // init database
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 1000")
		DBCreator.createDB(db)
    }

    public Audit() {

    }


    public processSeenBlock(currentBlock) {
        log4j.info("Block ${currentBlock}: seen")

        db.execute("insert into mastercoinBlocks values (${currentBlock}, 'seen', 0)")
    }


    public lastProcessedBlock() {
        def Long result

        def row = db.firstRow("select max(blockId) from mastercoinBlocks where status in ('processed','error')")

        assert row != null

        if (row[0] == null) {
            db.execute("insert into mastercoinBlocks values(${inceptionBlock}, 'processed', 0)")
            result = inceptionBlock
        }
        else {
            result = row[0]
        }

        return result
    }


    public lastBlock() {
        def Long result

        def row = db.firstRow("select max(blockId) from mastercoinBlocks")

        assert row != null

        if (row[0] == null) {
            result = inceptionBlock
        }
        else {
            result = row[0]
        }

        return result
    }



	// Should check the asset name/id's
    public processBlock(Long currentBlock) {
        def timeStart
        def timeStop
        def duration
        def sends
        def issuances

        timeStart = System.currentTimeMillis()
        sends = mastercoinAPI.getSends(currentBlock)

        // Process sends
        log4j.info("Block ${currentBlock}: processing " + sends.size() + " sends")
        def transactions = []
        for (send in sends) {
            //assert send instanceof HashMap
            def inputAddresses = []
            def outputAddresses = []
            def inAsset = ""
            def Long inAmount = 0
            def outAsset = ""
            def Long outAmount = 0
            def fee = 0.0
            def txFee = 0.0
            def txid = ""
            def source = send.sendingaddress
            def destination = send.referenceaddress			
            def serviceAddress = "" // the service address which was sent to
			def outAssetType = ""

            // Check if the send was performed to the central service listen to any of the central asset addresses we are listening on
            def notFound = true
            def counter = 0
            while (notFound && counter <= assetConfig.size()-1) {
                if (send.sendingaddress != assetConfig[counter].mastercoinAddress && send.referenceaddress == assetConfig[counter].mastercoinAddress && send.direction == "in") {
                    notFound = false
                    inAsset = assetConfig[counter].mastercoinAssetName
                    outAsset = assetConfig[counter].nativeAssetName
		    outAssetType = Asset.NATIVE_TYPE
                    fee = assetConfig[counter].feePercentage
                    txFee = assetConfig[counter].txFee
                    serviceAddress = assetConfig[counter].mastercoinAddress
                } else if (send.source != assetConfig[counter].mastercoinToCounterpartyAddress && send.destination == assetConfig[counter].mastercoinToCounterpartyAddress) { 
                    notFound = false
                    inAsset = assetConfig[counter].mastercoinAssetName
                    outAsset = assetConfig[counter].counterpartyAssetName
                    fee = assetConfig[counter].feePercentage
                    txFee = assetConfig[counter].txFee
                    serviceAddress = assetConfig[counter].mastercoinToCounterpartyAddress
					outAssetType = Asset.COUNTERPARTY_TYPE
				}

                counter++
            }


	    def calculatedFee

            // Record the send
            if (notFound == false) {
				if (outAssetType == Asset.NATIVE_TYPE) {
					txid = send.tx_hash
					inAmount = send.quantity			                   

					// Calculate fee	
					def amountMinusTX

					// Remove the TX Fee first from calculations
					amountMinusTX = inAmount - (txFee * satoshi)

					// If the amount that was sent was less than the cost of TX then eat the whole amount
					if (amountMinusTX < 0) {
						amountMinusTX = 0
					}

					if (amountMinusTX > 0) {
						calculatedFee = ((amountMinusTX * fee / 100) + (txFee * satoshi)).toInteger()

						if (inAmount < calculatedFee) {
							calculatedFee = inAmount
						}
					}
					else {
						calculatedFee = inAmount
					}

					// Set out amount if it is more than a satoshi
					if (inAmount - calculatedFee >= 1) {
						outAmount = inAmount - calculatedFee
					}
					else {
						outAmount = 0
					}

					assert inAmount == outAmount + calculatedFee  // conservation of energy
				} else {	
					// TODO implement exchange
				}
                inputAddresses.add(source)
                outputAddresses.add(destination)

                transactions.add([txid, inputAddresses, outputAddresses, inAmount, inAsset, outAmount, outAsset, calculatedFee, serviceAddress,outAssetType])
                log4j.info("Block ${currentBlock} found service call: ${currentBlock} ${txid} ${inputAddresses} ${serviceAddress} (${outAssetType}) ${inAmount/satoshi} ${inAsset} -> ${outAmount/satoshi} ${outAsset} (${calculatedFee/satoshi} ${inAsset} fee collected)")
            }
        }

        // Insert into DB the transaction along with the order details
        db.execute("begin transaction")
        try {
            for (transaction in transactions) {
                def String txid = transaction[0]
                def String inputAddress = transaction[1][0]            // pick the first input address if there is more than 1
                def String serviceAddress = transaction[8]  		   // The address to which asset was sent
                def Long inAmount = transaction[3]
                def inAsset = transaction[4]
                def Long outAmount = transaction[5]
                def String outAsset = transaction[6]
                def feeAmount = transaction[7]
                def feeAsset = inAsset
				def String outAssetType = transaction[9]

                // int currentBlockValue, String txidValue, String sourceAddressValue, String destinationAddressValue, String outAssetValue, String outAmountValue, int lastModifiedBlockIdValue, int originalAmount
                // there will only be 1 output for mastercoin assets but not the case for native assets - ie change
                // form a payment object which will determine the payment direction and source and destination addresses
                // public Payment(int currentBlockValue, String txidValue, String sourceAddressValue, String destinationAddressValue, String outAssetValue, Long outAmountValue, int lastModifiedBlockIdValue, Long originalAmount)
                def payment = new Payment(inAsset, currentBlock, txid, inputAddress, serviceAddress, outAsset, outAssetType, outAmount, currentBlock, inAmount)

                log4j.info("insert into transactions values (${currentBlock}, ${txid})")
                db.execute("insert into transactions values (${currentBlock}, ${txid})")
                for (eachInputAddress in transaction[1]) {
                    log4j.info("insert into inputAddresses values (${txid}, ${eachInputAddress})")
                    db.execute("insert into inputAddresses values (${txid}, ${eachInputAddress})")
                }
                for (outputAddress in transaction[2]) {
                    log4j.info("insert into outputAddresses values (${txid}, ${outputAddress})")
                    db.execute("insert into outputAddresses values (${txid}, ${outputAddress})")
                }

                log4j.info("insert into credits values (${currentBlock}, ${txid}, ${inputAddress}, ${serviceAddress}, ${inAsset}, ${inAmount}, ${outAsset}, ${outAmount}, 'valid')")
                db.execute("insert into credits values (${currentBlock}, ${txid}, ${inputAddress}, ${serviceAddress}, ${inAsset}, ${inAmount}, ${outAsset}, ${outAmount}, 'valid')")
                log4j.info("insert into fees values (${currentBlock}, ${txid}, ${feeAsset}, ${feeAmount})")
                db.execute("insert into fees values (${currentBlock}, ${txid}, ${feeAsset}, ${feeAmount} )")
                if (outAmount > 0) {
                    log4j.info("insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, ${Asset.MASTERCOIN_TYPE}, ${payment.destinationAddress}, ${payment.outAsset}, ${payment.outAssetType}, ${payment.outAmount}, ${payment.status}, ${payment.lastModifiedBlockId})")
                    db.execute("insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, ${Asset.MASTERCOIN_TYPE}, ${payment.destinationAddress}, ${payment.outAsset}, ${payment.outAssetType}, ${payment.outAmount}, ${payment.status}, ${payment.lastModifiedBlockId})")
                }
            }

            db.execute("commit transaction")
        } catch (Throwable e) {
            db.execute("update mastercoinBlocks set status='error', duration=0 where blockId = ${currentBlock}")
            log4j.info("Block ${currentBlock}: error")
            log4j.info("Exception: ${e}")
            db.execute("rollback transaction")
            assert e == null
        }

        timeStop = System.currentTimeMillis()
        duration = (timeStop-timeStart)/1000
        db.execute("update mastercoinBlocks set status='processed', duration=${duration} where blockId = ${currentBlock}")
        log4j.info("Block ${currentBlock}: processed in ${duration}s")

    }

    public static int main(String[] args) {
        def mastercoinFollower = new MastercoinFollower()

        mastercoinFollower.init()
        mastercoinFollower.Audit()

        log4j.info("mastercoind follower started")
        log4j.info("Last processed block: " + mastercoinFollower.lastProcessedBlock())
        log4j.info("Last seen block: " + mastercoinFollower.lastBlock())

        // Begin following blocks
        while (true) {
            def blockHeight = bitcoinAPI.getBlockHeight()
            def currentBlock = mastercoinFollower.lastBlock()
            def currentProcessedBlock = mastercoinFollower.lastProcessedBlock()

            // If the current block is less than the last block we've seen then add it to the blocks db
            while (mastercoinFollower.lastBlock() < blockHeight) {
                currentBlock++

                mastercoinFollower.processSeenBlock(currentBlock)
            }

            // Check if we can process a block
            while (mastercoinFollower.lastProcessedBlock() < currentBlock - confirmationsRequired) {
                currentProcessedBlock++

                mastercoinFollower.processBlock(currentProcessedBlock)

                currentProcessedBlock = mastercoinFollower.lastProcessedBlock()
            }

            sleep(sleepIntervalms)
        }

    }
}
