/**
 * Created by whoisjeremylam on 14/03/14.
 */

import org.apache.log4j.*
import org.sqlite.SQLite
import groovy.sql.Sql

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')


public class VenndNativeFollower {
    static logger
    static log4j
    static bitcoinAPI
    static satoshi = 100000000
    static boolean outAssetDivisible
    static boolean outAssetIssuanceDependent
    static int inceptionBlock
    static BigDecimal feeAmountPercentage
    static assetConfig
    static boolean testMode
    static int confirmationsRequired
    static int sleepIntervalms
    static String outAssetNonDivisibleRoundRule
    static Long outAssetMultiplier
    static String databaseName
    static db

    public static class Payment {
        def String inAsset
        def Long currentBlock
        def String txid
        def String sourceAddress
        def String destinationAddress
        def String outAsset
		def String outAssetType
        def Long outAmount
        def Long lastModifiedBlockId
        def String status
        def Long refundAmount = 0
        def String issuanceStatus

        public Payment(String inAssetValue, Long currentBlockValue, String txidValue, String sourceAddressValue, String destinationAddressValue, String outAssetValue, String outAssetTypeValue, Long outAmountValue, Long lastModifiedBlockIdValue, Long originalAmount, boolean unclearSource) {
            def row
            inAsset = inAssetValue
            outAsset = outAssetValue
			outAssetType = outAssetTypeValue
			
            outAmount = outAmountValue * outAssetMultiplier
            // Treat indivisible asset differently as they aren't multiplied by the satoshi factor
            if (!outAssetDivisible) {
                def Float outAmountFloat = outAmount / satoshi

                if (outAssetNonDivisibleRoundRule == 'floor') {
                    outAmount = Math.floor(outAmountFloat)
                    refundAmount = 0
                }
                else if (outAssetNonDivisibleRoundRule == 'ceiling') {
                    outAmount = Math.ceil(outAmountFloat)
                    refundAmount = 0
                }
                else if (outAssetNonDivisibleRoundRule == 'roundRefund') {
                    outAmount = Math.round(outAmountFloat)
                    refundAmount = 0 // to do
                }
                else {
                    outAmount = Math.round(outAmountFloat)
                    refundAmount = 0
                }

            }
            currentBlock = currentBlockValue
            txid = txidValue
            lastModifiedBlockId = lastModifiedBlockIdValue

//            if (originalAmount <= satoshi) {
			//No more than 10 BTC for now...:)
            if (originalAmount <= satoshi*10) {
                status = 'authorized'
            }
            else {
                status = 'valid'
            }

            // If a precise amount of asset must be issued each time, payments must wait until the asset is issued
            if (outAssetIssuanceDependent) {
                issuanceStatus = status
                status = 'waitIssuance'
            }

            // If the source address appeared to come from a wallet which isn't Counterwallet, default the payment to manual
            if (unclearSource) {
                status = 'manual'
            }
        }
    }


    public init() {
        bitcoinAPI = new BitcoinAPI()

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("VenndNativeFollower_log4j.properties")
        log4j = logger.getRootLogger()

        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("VenndNativeFollower.ini").toURL())
        outAssetDivisible = iniConfig.outAssetDivisible
        outAssetIssuanceDependent = iniConfig.outAssetIssuanceDependent
        inceptionBlock = iniConfig.inceptionBlock
        feeAmountPercentage = iniConfig.feeAmountPercentage
        testMode = iniConfig.testMode
        sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        confirmationsRequired = iniConfig.confirmationsRequired
        outAssetNonDivisibleRoundRule = iniConfig.outAssetNonDivisibleRoundRule
        outAssetMultiplier = iniConfig.outAssetMultiplier

        assetConfig = Asset.readAssets("AssetInformation.ini")


        // Init database
        def row
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 1000")
		DBCreator.createDB(db)
    }


    // Checks balances in wallet versus what is in the DB tables
    public audit() {

    }


    static public lastProcessedBlock() {
        def Long result

        def row = db.firstRow("select max(blockId) from blocks where status in ('processed','error')")

        assert row != null

        if (row[0] == null) {
            db.execute("insert into blocks values(${inceptionBlock}, 'processed', 0)")
            result = inceptionBlock
        }
        else {
            result = row[0]
        }

        assert result > 0
        return result
    }


    static public lastBlock() {
        def Long result

        def row = db.firstRow("select max(blockId) from blocks")

        assert row != null

        if (row[0] == null) {
            result = inceptionBlock
        }
        else {
            result = row[0]
        }

        return result
    }


    static processSeenBlock(currentBlock) {
        println "Block ${currentBlock}: seen"
        db.execute("insert into blocks values (${currentBlock}, 'seen', 0)")
    }


    static processBlock(currentBlock) {
        def timeStart
        def timeStop

        timeStart = System.currentTimeMillis()
        def blockHash = bitcoinAPI.getBlockHash(currentBlock)
        def block = bitcoinAPI.getBlock(blockHash)

        println "Block ${currentBlock}: processing " + block['tx'].size() + " transactions"

        def result
        def rawtransactions = []
        def count = 1
        for (tx in block['tx']) {
            def txid = tx

            result = bitcoinAPI.getRawTransaction(txid)
            rawtransactions.add(result)

            count++

            if (testMode && count > 10) {
                break
            }
        }

        // Iterate through each raw transaction and get the parsed transaction by calling decoderawtransaction
        def parsedTransactions = []
        for (rawTransaction in rawtransactions) {
            def notCounterwalletSend = false
            def inputAddresses = []
            def outputAddresses = []
            def amounts = [] //same position as address in outputAddresses
            def Long inAmount = 0
            def Long outAmount = 0
            def decodedTransaction = bitcoinAPI.getTransaction(rawTransaction)
            def txid = decodedTransaction.txid
            def Asset asset 
			def serviceAddress = "" 
			def String type = "" 

            // Add output addresses
            for (vout in decodedTransaction.vout) {
                def address
                if (vout.scriptPubKey.addresses != null) {
                    address = vout.scriptPubKey.addresses[0]
                    def amount = vout.value
                    outputAddresses.add(address)
                    amounts.add(amount)
                }
                else {
                    outputAddresses.add("Unable to decode address")
                    amounts.add(0)
                }
            }

            // Check if the send was performed to the central service listen to any of the central asset addresses we are listening on
            def found = false
			def outAsset = ""
			for (assetRec in assetConfig) {
				if (outputAddresses.contains(assetRec.nativeAddressMastercoin)) 
					serviceAddress = assetRec.nativeAddressMastercoin
					found = true
					asset = assetRec
					type = Asset.MASTERCOIN_TYPE
					outAsset = asset.mastercoinAssetName 
				} else if (outputAddresses.contains(assetRec.nativeAddressCounterparty) { 
					serviceAddress = assetRec.nativeAddressCounterparty
					found = true
					asset = assetRec
					type = Asset.COUNTERPARTY_TYPE
					outAsset = asset.counterpartyAssetName 
				} 
			}

            // Record the send
            if (found == true) {
                def listenerAddressIndex = outputAddresses.findIndexOf {it == serviceAddress}
                inAmount = amounts[listenerAddressIndex] * satoshi
                assert inAmount > 0


                // Calculate fee
                def amountMinusTX
                def calculatedFee

                // Remove the TX Fee first from calculations
                amountMinusTX = inAmount - (asset.txFee * satoshi)

                // If the amount that was sent was less than the cost of TX then eat the whole amount
                if (amountMinusTX < 0) {
                    amountMinusTX = 0
                }

                if (amountMinusTX > 0) {
                    calculatedFee = ((amountMinusTX * feeAmountPercentage / 100) + (asset.txFee * satoshi)).toInteger()

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

                //Get input addresses
                for (vin in decodedTransaction.vin) {
                    def position = vin.vout
                    def input_txid = vin.txid

                    // If there are inputs to this tx then look them up
                    if (input_txid != null) {
                        def rawInputTransaction = bitcoinAPI.getRawTransaction(input_txid)
                        def sourceInputTransaction = bitcoinAPI.getTransaction(rawInputTransaction)
                        def inputAddress = sourceInputTransaction.vout[position].scriptPubKey.addresses[0]
                        inputAddresses.add(inputAddress)
                    }
                }

                // Check if this send was a counterwallet send
                def uniqueInputAddresses = inputAddresses.unique()
                if (uniqueInputAddresses.size() > 1) {
                    notCounterwalletSend = true
                }
                def uniqueChangeAddresses = outputAddresses.unique()
                def uniqueChangeAddressesWithoutInput = outputAddresses - uniqueInputAddresses[0] // for a counterwallet, after removing change address, send this should only leave 1 destination address
                if (uniqueChangeAddressesWithoutInput.size() > 1) {
                    notCounterwalletSend = true
                }

                // Only record if one of the input addresses is NOT the service address. ie we didn't initiate the send
                // if (inputAddresses.contains(listenerAddress) == false) {
                parsedTransactions.add([txid, inputAddresses, outputAddresses, inAmount, asset.nativAssetName, outAmount, outAsset, calculatedFee, serviceAddress, type, notCounterwalletSend])
                println "Block ${currentBlock} found service call: ${currentBlock} ${txid} ${inputAddresses} ${outputAddresses} ${inAmount/satoshi} ${inAsset} -> ${outAmount/satoshi} ${outAsset} (${calculatedFee/satoshi} ${inAsset} fee collected)"
                //}
            }


            timeStop = System.currentTimeMillis()
        }

        // Insert into DB the transaction along with the order details
        db.execute("begin transaction")
        try {
            for (transaction in parsedTransactions) {
                def txid = transaction[0]
                def inputAddress = transaction[1][0]            // pick the first input address if there is more than 1
                def String serviceAddress = transaction[8]  // take the service address as the address that was sent to
                def inAmount = transaction[3]
                def inAsset = transaction[4]
                def outAmount = transaction[5]
                def outAsset = transaction[6]
                def feeAmount = transaction[7]
				def outAssetType = transaction[9]
                def notCounterwalletSend = transaction[10]
                def feeAsset = inAsset

                // there will only be 1 output for counterparty assets but not the case for native assets - ie change
                // form a payment object which will determine the payment direction and source and destination addresses
                def payment = new Payment(inAsset, currentBlock, txid, inputAddress, serviceAddress, outAsset,outAssetType, outAmount, currentBlock, inAmount, notCounterwalletSend)

                println "insert into transactions values (${currentBlock}, ${txid})"
                db.execute("insert into transactions values (${currentBlock}, ${txid})")
                for (eachInputAddress in transaction[1]) {
                    println "insert into inputAddresses values (${txid}, ${eachInputAddress})"
                    db.execute("insert into inputAddresses values (${txid}, ${eachInputAddress})")
                }
                for (outputAddress in transaction[2]) {
                    println "insert into outputAddresses values (${txid}, ${outputAddress})"
                    db.execute("insert into outputAddresses values (${txid}, ${outputAddress})")
                }
				
				// TODO handle type
                println "insert into credits values (${currentBlock}, ${txid}, ${inputAddress}, '', ${inAsset}, ${inAmount}, ${outAsset}, ${outAmount}, 'valid')"
                db.execute("insert into credits values (${currentBlock}, ${txid}, ${inputAddress}, '', ${inAsset}, ${inAmount}, ${outAsset}, ${outAmount}, 'valid')")
                println "insert into fees values (${currentBlock}, ${txid}, ${feeAsset}, ${feeAmount})"
                db.execute("insert into fees values (${currentBlock}, ${txid}, ${feeAsset}, ${feeAmount})")
                if (payment.outAmount > 0) {
                    if (outAssetIssuanceDependent) {
						// TODO handle type
                        // create table if not exists issuances(blockId integer, sourceTxid string, sourceAddress string, asset string, amount integer, divisibility string, status string, lastUpdatedBlockId integer)
                        println "insert into issuances values (${currentBlock}, ${txid}, ${inputAddress}, ${outAsset}, ${payment.outAmount}, ${outAssetDivisible}, ${payment.issuanceStatus}, ${currentBlock})"
                        db.execute("insert into issuances values (${currentBlock}, ${txid}, ${inputAddress}, ${outAsset}, ${payment.outAmount}, ${outAssetDivisible}, ${payment.issuanceStatus}, ${currentBlock})")
                    }

                    println "insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, Asset.NATIVE_TYPE,  ${payment.destinationAddress}, ${payment.outAsset}, ${payment.outAssetType}, ${payment.outAmount}, ${payment.status}, ${payment.lastModifiedBlockId})"
                    db.execute("insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, Asset.NATIVE_TYPE, ${payment.destinationAddress}, ${payment.outAsset}, ${payment.outAssetType}, ${payment.outAmount}, ${payment.status}, ${payment.lastModifiedBlockId})")

                    // process a refund
                    if (payment.refundAmount > 0) {
                        println "insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, Asset.NATIVE_TYPE, ${payment.destinationAddress}, ${payment.inAsset}, Asset.NATIVE_TYPE, ${payment.refundAmount}, ${payment.status}, ${payment.lastModifiedBlockId}) -- refund"
                        db.execute("insert into payments values (${payment.currentBlock}, ${payment.txid}, ${payment.sourceAddress}, Asset.NATIVE_TYPE, ${payment.destinationAddress}, ${payment.inAsset}, Asset.NATIVE_TYPE, ${payment.refundAmount}, ${payment.status}, ${payment.lastModifiedBlockId})")
                    }
                }
            }
            db.execute("commit transaction")
        } catch (Throwable e) {
            db.execute("update blocks set status='error', duration=0 where blockId = ${currentBlock}")
            println "Block ${currentBlock}: error"
            println "Exception: ${e}"
            db.execute("rollback transaction")
            assert e == null
        }

        def duration = (timeStop-timeStart)/1000
        db.execute("update blocks set status='processed', duration=${duration} where blockId = ${currentBlock}")
        println "Block ${currentBlock}: processed in ${duration}s"
    }


    public static int main(String[] args) {
        def venndNativeFollower = new VenndNativeFollower()

        venndNativeFollower.init()
        venndNativeFollower.audit()

        println "native API daemon follower started"
        println "Last processed block: " + lastProcessedBlock()
        println "Last seen block: " + lastBlock()

        // Begin following blocks
        while (true) {
            def blockHeight = bitcoinAPI.getBlockHeight()
            def currentBlock = lastBlock()
            def currentProcessedBlock = lastProcessedBlock()

            // If the current block is less than the last block we've seen then add it to the blocks db
            while (lastBlock() < blockHeight) {
                currentBlock++

                processSeenBlock(currentBlock)

                currentBlock = lastBlock() // this value should stay the same
            }

            // Check if we can process a block
            while (lastProcessedBlock() < currentBlock - confirmationsRequired) {
                currentProcessedBlock++

                processBlock(currentProcessedBlock)

                currentProcessedBlock = lastProcessedBlock()
            }

            sleep(sleepIntervalms)
        }


    } // end main
} // end class