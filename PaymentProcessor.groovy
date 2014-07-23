/**
 * Created by jeremy on 1/04/14.
 */

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')

import org.apache.log4j.*
import groovy.sql.Sql

class PaymentProcessor {
    static counterpartyAPI
	static mastercoinAPI
    static bitcoinAPI
    static boolean testMode
    static String walletPassphrase
    static int sleepIntervalms
    static String databaseName
    static String counterpartyTransactionEncoding
    static int walletUnlockSeconds
	static satoshi = 100000000

    static logger
    static log4j
    static db


    class Payment {
        def blockIdSource
        def txid
        def sourceAddress
        def destinationAddress
        def outAsset
		def outAssetType
        def outAmount
        def status
        def lastUpdatedBlockId
		def inAssetType

        public Payment(blockIsSourceValue, txidValue, sourceAddressValue, inAssetTypeValue, destinationAddressValue, outAssetValue, outAssetTypeValue, outAmountValue, statusValue, lastUpdatedBlockIdValue) {
            blockIdSource = blockIsSourceValue
            txid = txidValue
            sourceAddress = sourceAddressValue
            destinationAddress = destinationAddressValue
            outAsset = outAssetValue
			outAssetType = outAssetTypeValue
			inAssetType = inAssetTypeValue
            outAmount = outAmountValue
            status = statusValue
            lastUpdatedBlockId = lastUpdatedBlockIdValue
        }
    }


    public init() {

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("PaymentProcessor_log4j.properties")
        log4j = logger.getRootLogger()
        log4j.setLevel(Level.INFO)
		
		counterpartyAPI = new CounterpartyAPI(log4j)
		mastercoinAPI = new MastercoinAPI(log4j)
        bitcoinAPI = new BitcoinAPI()

        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("PaymentProcessor.ini").toURL())
        testMode = iniConfig.testMode
        walletPassphrase = iniConfig.bitcoin.walletPassphrase
        sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        counterpartyTransactionEncoding = iniConfig.counterpartyTransactionEncoding
        walletUnlockSeconds = iniConfig.walletUnlockSeconds

        // Init database
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 2000")
		DBCreator.createDB(db)
    }


    public audit() {

    }

    public getLastPaymentBlock() {
        def Long result
        def row

        row = db.firstRow("select max(lastUpdatedBlockId) from payments where status in ('complete')")

        if (row == null) {
            result = 0
        }
        else {
            if (row[0] == null) result = 0
            else result = row[0]
        }

        return result
    }


    public getNextPayment() {
        def Payment result
        def row

        row = db.firstRow("select * from payments where status='authorized' order by blockId")

        if (row == null) result = null
        else {
            if (row[0] == null) result = null
            else {
                def blockIdSource = row.blockId
                def txid = row.SourceTxid
                def sourceAddress = row.sourceAddress
                def destinationAddress = row.destinationAddress
                def outAsset = row.outAsset
                def outAmount = row.outAmount
                def status = row.status
                def lastUpdated = row.lastUpdatedBlockId
				def outAssetType = row.outAssetType
				def inAssetType = row.inAssetType
				
                result = new Payment(blockIdSource, txid, sourceAddress, inAssetType, destinationAddress, outAsset, outAssetType, outAmount, status, lastUpdated)
            }
        }

        return result
    }
	
	def get_total_counterparty(String asset) { 
		// Calculate the required dividend
        def getAssetInfo = counterpartyAPI.getAssetInfo(asset)

        //assert getAssetInfo instanceof java.lang.String
        //assert getAssetInfo != null
        //if (!(getAssetInfo instanceof java.lang.String)) { // catch non technical error in RPC call
        //    assert getAssetInfo.code == null
        //}
        log4j.info("Processing counterparty dividend getAssetInfo.supply ${getAssetInfo.supply}")
		return getAssetInfo.supply[0]
	}
	
	def get_total_mastercoin(String asset) {
		def getAssetInfo = mastercoinAPI.getAssetInfo(asset)
		log4j.info("Processing mastercoin dividend getAssetInfo.totaltokens ${getAssetInfo.totaltokens}")
		return getAssetInfo.totaltokens
	}
	
	def get_counterparty_notused(String sourceAddress, String asset) {
		def balances = counterpartyAPI.getBalances(sourceAddress)
		def asset_balaance = 0


        for (balance in balances) {
            if (balance.asset == dividend_asset) 
                asset_balance = balance.quantity
        }
		
		return asset_balance
	}
	
	def get_mastercoin_notused(String sourceAddress, String asset) {
		def balance = mastercoinAPI.getBalance(sourceAddress,asset)
		return balance.result
	}

    public pay_dividend(Long currentBlock, Payment payment,Long dividend_percent)
    {
        def counterparty_sourceAddress = payment.sourceAddress // TODO 
		def mastercoin_sourceAddress  = payment.sourceAddress // TODO 
        def blockIdSource = payment.blockIdSource
        def asset = payment.inAsset
        def dividend_asset = payment.outAsset
        def amount = payment.outAmount
        def asset_balance = 0
       
        log4j.info("Processing dividend payment ${payment.blockIdSource} ${payment.txid}. Sending dividend_percent ${dividend_percent } ")
        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)
 
        def counterparty_numberOfTokenIssued = get_total_counterparty(dividend_asset) 
		def counterparty_asset_balance = get_counterparty_notused(sourceAddress)
		def counterparty_tokensOutThere = counterparty_numberOfTokenIssued-counterparty_asset_balance
		
		def mastercoin_numberOfTokenIssued = get_total_mastercoin(dividend_asset) 
		def mastercoin_asset_balance = get_mastercoin_notused(sourceAddress)
		def mastercoin_tokensOutThere = mastercoin_numberOfTokenIssued-mastercoin_asset_balance
		
		def totalTokens = mastercoin_tokensOutThere + counterparty_tokensOutThere
		
		
		//////////////////////////////////////////////////////////////////////////// BTC
		def counterparty_fraction = counterparty_tokensOutThere / (counterparty_tokensOutThere + mastercoin_tokensOutThere)

        log4j.info("pay_dividend in counterparty asset_balance ${counterparty_asset_balance} numberOfTokenIssued = ${counterparty_numberOfTokenIssued} tokensOutThere = ${counterparty_tokensOutThere} ")
		log4j.info("pay_dividend in mastercoin asset_balance ${mastercoin_balance} numberOfTokenIssued = ${mastercoin_numberOfTokenIssued} tokensOutThere = ${mastercoin_tokensOutThere} ")
        def quantity_per_share_dividend = Math.round(((amount*dividend_percent)/100)/(totalTokens)*satoshi)
        log4j.info("pay_dividend asset ${dividend_asset} asset_balance= ${asset_balance} tokensOutThere = ${totalTokens} quantity_per_share_dividend = ${quantity_per_share_dividend} ")

        // Create the (unsigned) counterparty dividend transaction    
        def counterparty_unsignedTransaction = counterpartyAPI.sendDividend(sourceAddress, quantity_per_share_dividend, asset,dividend_asset)		
        assert counterparty_unsignedTransaction instanceof java.lang.String
        assert counterparty_unsignedTransaction != null
        if (!(unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
            assert counterparty_unsignedTransaction.code == null
        }

        // sign transaction
        def counterparty_signedTransaction = counterpartyAPI.signTransaction(counterparty_unsignedTransaction)
        assert counterparty_signedTransaction instanceof java.lang.String
        assert counterparty_signedTransaction != null

        // send transaction
        try {
            counterpartyAPI.broadcastSignedTransaction(counterparty_signedTransaction)
			mastercoinAPI.sendDividend(mastercoin_sourceAddress, quantity_per_share_dividend * mastercoin_numberOfTokenIssued, asset, dividend_asset) // TODO check
            log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
            // db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
        }
        catch (Throwable e) {
            log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
            db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
			
            assert e == null
        }

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        log4j.info("Payment dividend ${sourceAddress} -> quantity_per_share_dividend ${quantity_per_share_dividend/satoshi} ${asset} complete")
        if (testMode == true) log4j.info("Test mode: Payment 12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7 -> 142UYTzD1PLBcSsww7JxKLck871zRYG5D3 " + 20000/satoshi + "${asset} complete")

//        return unsignedTransaction

    }
	
	public pay(Long currentBlock, Payment payment,Long dividend_percent) {
		
        def sourceAddress = payment.sourceAddress
        def blockIdSource = payment.blockIdSource
        def destinationAddress = payment.destinationAddress
        def asset = payment.outAsset
        def amount = payment.outAmount
		
        log4j.info("amount= {$amount} dividend_percent={$dividend_percent}")

        // Calculate the payment to the address after reducing the divided
		// TODO check rounding 
        amount = amount*((100-dividend_percent)/100) 

        amount = Math.round(amount)

        log4j.info("amount= after {$amount} ")


        log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid}. Sending ${payment.outAmount / satoshi} ${payment.outAsset} from ${payment.sourceAddress} to ${payment.destinationAddress}")

        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)

		if (payment.outAssetType == Asset.COUNTERPARTY_TYPE) {
			// Create the (unsigned) counterparty send transaction
			def unsignedTransaction = counterpartyAPI.createSend(sourceAddress, destinationAddress, asset, amount, testMode)
			assert unsignedTransaction instanceof java.lang.String
			assert unsignedTransaction != null
			if (!(unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
				assert unsignedTransaction.code == null
			}

			// sign transaction
			def signedTransaction = counterpartyAPI.signTransaction(unsignedTransaction)
			assert signedTransaction instanceof java.lang.String
			assert signedTransaction != null

			// send transaction
			try {
				counterpartyAPI.broadcastSignedTransaction(signedTransaction)
				log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
			}
			catch (Throwable e) {
				log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
	
				assert e == null
			}
		} else {
			// send transaction
			try {
				mastercoinAPI.sendAsset(sourceAddress, destinationAddress, asset, amount, testMode)
				log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
			}
			catch (Throwable e) {
				log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
				db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
	
				assert e == null
			}
		}

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        log4j.info("Payment ${sourceAddress} -> ${destinationAddress} ${amount/satoshi} ${asset} complete")
        if (testMode == true) log4j.info("Test mode: Payment 12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7 -> 142UYTzD1PLBcSsww7JxKLck871zRYG5D3 " + 20000/satoshi + "${asset} complete")

//        return unsignedTransaction
    }


	// We don't really use the current block... 
    // This is the major thing that needs to be fixed. We shall assume that we have different addresses,
	// so that we can discover... 
	public static int main(String[] args) {
        def paymentProcessor = new PaymentProcessor()
		def Long dividend_percent = 10

        paymentProcessor.init()
        paymentProcessor.audit()

        log4j.info("Payment processor started")
        log4j.info("Last processed payment: " + paymentProcessor.getLastPaymentBlock())
		// counterpartyAPI.getBalances("sdfsdf")

        // Begin following blocks
        while (true) {
            def blockHeight = bitcoinAPI.getBlockHeight()
            def lastPaymentBlock = paymentProcessor.getLastPaymentBlock()
            def Payment payment = paymentProcessor.getNextPayment()

            assert lastPaymentBlock <= blockHeight

            log4j.info("Block ${blockHeight}")

//            // If a payment wasn't performed this block and there is a payment to make
//            if (lastPaymentBlock < blockHeight && payment != null) {
//                paymentProcessor.pay(blockHeight, payment)
//            }
//            if (lastPaymentBlock >= blockHeight && payment != null) {
//                log4j.info("Payment to make but already paid already this block. Sleeping...")
//            }
            if (payment != null) {
                if (payment.inAssetType == Asset.NATIVE_TYPE){
					if (payment.outAssetType != Asset.NATIVE_TYPE){
						// This is an issuing transaction, we need to pay dividend
						log4j.info("payment.outAsset ${payment.outAsset}")
						paymentProcessor.pay_dividend(blockHeight, payment,dividend_percent)
						paymentProcessor.pay(blockHeight, payment,dividend_percent)
					} else {
						// This is a refund transaction... 
						// TODO 
					}					
				} else if (payment.outAssetType == Asset.NATIVE_TYPE) {
					// This is a sell transaction... 
					paymentProcessor.pay(blockHeight, payment,0)
					log4j.info("--------------BURN-------------")
				} else if ((payment.inAssetType == Asset.MASTERCOIN_TYPE && payment.outAssetType == Asset.COUNTERPARTY_TYPE ) || 
					(payment.inAssetType == Asset.COUNTERPARTY_TYPE && payment.outAssetType == Asset.MASTERCOIN_TYPE)) {
					// TODO This is an exchange transaction
					paymentProcessor.pay(blockHeight, payment,0)
					
					log4j.info("----------------- EXCHANGE TRANSACTION -----------------")
				} else {				
					log4j.info("----------------- UNKOWN TRANSACTION TYPE -----------------")
				}
				
                log4j.info("Sleeping...payment.outAsset ${payment.outAsset}")
            }
            else {
                log4j.info("No payments to make. Sleeping..${sleepIntervalms}.")
            }

            sleep(sleepIntervalms)
        }

    }
}
