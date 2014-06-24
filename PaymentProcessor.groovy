/**
 * Created by jeremy on 1/04/14.
 */

@Grab(group='log4j', module='log4j', version='1.2.17')
@Grab(group='org.xerial', module='sqlite-jdbc', version='3.7.2')

import org.apache.log4j.*
import groovy.sql.Sql

class PaymentProcessor {
    static counterpartyAPI
    static bitcoinAPI
    static boolean testMode
//    static String listenerBitcoinAddress
    static String walletPassphrase
    static int sleepIntervalms
    static String databaseName
    static String counterpartyTransactionEncoding
    static int walletUnlockSeconds

    static logger
    static log4j
//    static groovyx.net.http.AsyncHTTPBuilder HttpAsync
    static db


    class Payment {
        //blockIdSource integer, txid string, sourceAddress string, inAsset string, inAmount integer, outAsset string, outAmount integer, status string, lastUpdatedBlockId integer
        def blockIdSource
        def txid
        def sourceAddress
        def destinationAddress
        def outAsset
        def outAmount
        def status
        def lastUpdatedBlockId
        def inAsset


        public Payment(blockIsSourceValue, txidValue, sourceAddressValue, destinationAddressValue, outAssetValue,inAssetValue ,outAmountValue, statusValue, lastUpdatedBlockIdValue) {
            blockIdSource = blockIsSourceValue
            txid = txidValue
            sourceAddress = sourceAddressValue
            destinationAddress = destinationAddressValue
            outAsset = outAssetValue
            outAmount = outAmountValue
            status = statusValue
            lastUpdatedBlockId = lastUpdatedBlockIdValue
            inAsset = inAssetValue
        }
    }


    public init() {
        counterpartyAPI = new CounterpartyAPI()
        bitcoinAPI = new BitcoinAPI()

        // Set up some log4j stuff
        logger = new Logger()
        PropertyConfigurator.configure("PaymentProcessor_log4j.properties")
        log4j = logger.getRootLogger()
        log4j.setLevel(Level.INFO)

        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("PaymentProcessor.ini").toURL())
        testMode = iniConfig.testMode
//        listenerBitcoinAddress = iniConfig.listenerBitcoinAddress
        walletPassphrase = iniConfig.bitcoin.walletPassphrase
        sleepIntervalms = iniConfig.sleepIntervalms
        databaseName = iniConfig.database.name
        counterpartyTransactionEncoding = iniConfig.counterpartyTransactionEncoding
        walletUnlockSeconds = iniConfig.walletUnlockSeconds

        // Init database
        def row
        db = Sql.newInstance("jdbc:sqlite:${databaseName}", "org.sqlite.JDBC")
        db.execute("PRAGMA busy_timeout = 2000")

        // Check vital tables exist
        row = db.firstRow("select name from sqlite_master where type='table' and name='blocks'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='transactions'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='inputAddresses'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='outputAddresses'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='fees'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='audit'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='credits'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='debits'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='payments'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='issuances'")
        assert row != null
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
                //def inAsset = row.inAsset
                def inAsset = row.outAsset

                result = new Payment(blockIdSource, txid, sourceAddress, destinationAddress, outAsset,inAsset, outAmount, status, lastUpdated)
            }
        }

        return result
    }
  
    public pay_divident(Long currentBlock, Payment payment,Long dividend_percent)
    {
        def sourceAddress = payment.sourceAddress
        def blockIdSource = payment.blockIdSource
        def asset = payment.inAsset
        def dividend_asset = payment.outAsset
        def amount = payment.outAmount
        def asset_balance = 0

        

        log4j.info("Processing dividend payment ${payment.blockIdSource} ${payment.txid}. Sending dividend_percent ${dividend_percent } ")
        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)
        // Calculate the required dividend
        def getAssetInfo = counterpartyAPI.getAssetInfo(dividend_asset,log4j)

        //assert getAssetInfo instanceof java.lang.String
        //assert getAssetInfo != null
        //if (!(getAssetInfo instanceof java.lang.String)) { // catch non technical error in RPC call
        //    assert getAssetInfo.code == null
        //}
        log4j.info("Processing dividend getAssetInfo.supply ${getAssetInfo.supply}")
        def numberOfTokenIssued = getAssetInfo.supply
        def balances = counterpartyAPI.getBalances(sourceAddress)
        //assert balance instanceof java.lang.String
        //assert balance != null
        //if (!(balance instanceof java.lang.String)) { // catch non technical error in RPC call
          //  assert balance.code == null
        //}

        for (balance in balances) {
            if (balance.asset == dividend_asset) 
                asset_balance = balance.quantity
        }

        
        def tokensOutThere = numberOfTokenIssued[0]-asset_balance

        log4j.info("pay_divident asset_balance ${asset_balance} numberOfTokenIssued = ${numberOfTokenIssued} tokensOutThere = ${tokensOutThere} ")
        def quantity_per_share_divident = Math.round((((amount*dividend_percent)/100)/tokensOutThere)*100000000)
        log4j.info("pay_divident asset ${dividend_asset} asset_balance= ${asset_balance} tokensOutThere = ${tokensOutThere} quantity_per_share_divident = = ${quantity_per_share_divident} ")





        // Create the (unsigned) counterparty dividend transaction
    
        def unsignedTransaction = counterpartyAPI.sendDivident(sourceAddress, quantity_per_share_divident, asset,dividend_asset, log4j)
        assert unsignedTransaction instanceof java.lang.String
        assert unsignedTransaction != null
        if (!(unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
            assert unsignedTransaction.code == null
        }

        // sign transaction
        def signedTransaction = counterpartyAPI.signTransaction(unsignedTransaction, log4j)
        assert signedTransaction instanceof java.lang.String
        assert signedTransaction != null

        // send transaction
        try {
            counterpartyAPI.broadcastSignedTransaction(signedTransaction, log4j)
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

        log4j.info("Payment dividend ${sourceAddress} -> quantity_per_share_divident ${quantity_per_share_divident/100000000} ${asset} complete")
        if (testMode == true) log4j.info("Test mode: Payment 12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7 -> 142UYTzD1PLBcSsww7JxKLck871zRYG5D3 " + 20000/100000000 + "${asset} complete")

//        return unsignedTransaction

    }
    public pay(Long currentBlock, Payment payment,Long dividend_percent) {
        def sourceAddress = payment.sourceAddress
        def blockIdSource = payment.blockIdSource
        def destinationAddress = payment.destinationAddress
        def asset = payment.outAsset
        def amount = payment.outAmount

        log4j.info("amount= {$amount} dividend_percent={$dividend_percent}")

        //Calculate the payment to the address after reducing the divided
        amount = amount*((100-dividend_percent)/100) 

        amount = Math.round(amount)

        log4j.info("amount= after {$amount} ")

        log4j.info("Processing payment ${payment.blockIdSource} ${payment.txid}. Sending ${payment.outAmount / 100000000} ${payment.outAsset} from ${payment.sourceAddress} to ${payment.destinationAddress}")

        bitcoinAPI.lockBitcoinWallet() // Lock first to ensure the unlock doesn't fail
        bitcoinAPI.unlockBitcoinWallet(walletPassphrase, 30)

        // Create the (unsigned) counterparty send transaction

        def unsignedTransaction = counterpartyAPI.createSend(sourceAddress, destinationAddress, asset, amount, testMode, log4j)
        log4j.info("pay 2!")
        assert unsignedTransaction instanceof java.lang.String
        log4j.info("pay 3!")
        assert unsignedTransaction != null
        log4j.info("pay 4!")
        if (!(unsignedTransaction instanceof java.lang.String)) { // catch non technical error in RPC call
            log4j.info("pay 5!")
            assert unsignedTransaction.code == null
        }
        log4j.info("pay 1!")

        // sign transaction
        def signedTransaction = counterpartyAPI.signTransaction(unsignedTransaction, log4j)
        assert signedTransaction instanceof java.lang.String
        assert signedTransaction != null

        // send transaction
        try {
            counterpartyAPI.broadcastSignedTransaction(signedTransaction, log4j)
            log4j.info("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
            db.execute("update payments set status='complete', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
        }
        catch (Throwable e) {
            log4j.info("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")
            db.execute("update payments set status='error', lastUpdatedBlockId = ${currentBlock} where blockId = ${blockIdSource} and sourceTxid = ${payment.txid}")

            assert e == null
        }

        // Lock bitcoin wallet
        bitcoinAPI.lockBitcoinWallet()

        log4j.info("Payment ${sourceAddress} -> ${destinationAddress} ${amount/100000000} ${asset} complete")
        if (testMode == true) log4j.info("Test mode: Payment 12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7 -> 142UYTzD1PLBcSsww7JxKLck871zRYG5D3 " + 20000/100000000 + "${asset} complete")

//        return unsignedTransaction
    }
    

    public static int main(String[] args) {
        def paymentProcessor = new PaymentProcessor()
        def Long dividend_percent = 10

        paymentProcessor.init()
        paymentProcessor.audit()

        log4j.info("Payment processor started")
        log4j.info("Last processed payment: " + paymentProcessor.getLastPaymentBlock())

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
                if (payment.outAsset != 'BTC'){
                log4j.info("payment.outAsset ${payment.outAsset}")
                paymentProcessor.pay_divident(blockHeight, payment,dividend_percent)
                paymentProcessor.pay(blockHeight, payment,dividend_percent)

            }
            else
            {
                paymentProcessor.pay(blockHeight, payment,0)
                log4j.info("--------------BURN-------------")
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
