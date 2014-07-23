/**
 * Created by whoisjeremylam on 18/04/14.
 */

@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7' )

import groovyx.net.http.AsyncHTTPBuilder
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*

class MastercoinAPI {
    private String mastercoinTransactionEncoding
    private String mastercoinRpcURL
    private String mastercoinRpcUser
    private String mastercoinRpcPassword
    private groovyx.net.http.AsyncHTTPBuilder mastercoinHttpAsync
    private boolean mastercoinMultisendPerBlock
	private log4j

    private init() {
        // Read in ini file
        def iniConfig = new ConfigSlurper().parse(new File("MastercoinAPI.ini").toURL())

        mastercoinRpcURL = iniConfig.mastercoin.rpcURL
        mastercoinRpcUser = iniConfig.mastercoin.rpcUser
        mastercoinRpcPassword = iniConfig.mastercoin.rpcPassword

        mastercoinHttpAsync = new AsyncHTTPBuilder(
                poolSize : 10,
                uri : mastercoinRpcURL,
                contentType : JSON )
        mastercoinHttpAsync.auth.basic mastercoinRpcUser, mastercoinRpcPassword

    }
	
	private sendRPCMessage(command, in_params) {
		def paramsJSON
        def result = mastercoinHttpAsync.request( POST, JSON) { req ->
            body = [method : command,
                    id : 'test',
                    params : in_params,
                    jsonrpc : "2.0"
            ]

            paramsJSON = new groovy.json.JsonBuilder(body)
			log4j.info(command + " payload: " + paramsJSON)

            response.success = { resp, json ->
                if (json.containsKey("error")) {
                    log4j.info(command + " error: " + json.error)
                    return json.error
                }
				
                return json.result
            }
        }

        assert result instanceof java.util.concurrent.Future
        while ( ! result.done ) {
            Thread.sleep(100)
        }

        println result
        return result.get()
	}

    public getBalances(address) {    
		return sendRPCMessage('getallbalancesforaddress_MP', [address])
    }
	
	public getAssetBalance(address, asset) {
		return sendRPCMessage('getbalance_MP', [address, Integer.parseInt(asset, 10)])
	}

    public getSends(Long blockId) {
//		return sendRPCMessage('listblocktransactions_MP', [blockId,blockId])
		return sendRPCMessage('listtransactions_MP', ["*", 10000, 0, blockId,blockId])

	}

	// TODO cannot be done using mastercore 
    public getIssuances(Long blockId) {
		return null
	}

    public broadcastSignedTransaction(String signedTransaction) {
		return sendRPCMessage('sendrawtransaction', [signedTransaction])
	}
 
    public signTransaction(String unsignedTransaction) {
		return sendRPCMessage('signrawtransaction', [unsignedTransaction])
	}

	// Recall asset is integer in mastercoin
    public sendAsset(sourceAddress, destinationAddress, asset, amount, testMode) {
        def myParams

        if (testMode == false) {
            myParams = [sourceAddress, destinationAddress, Integer.parseInt(asset, 10), amount]
        }
        else {
			myParams = ['12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7', '142UYTzD1PLBcSsww7JxKLck871zRYG5D3', Integer.parseInt(asset, 10), 20000]
        }		
		return sendRPCMessage('send_MP', myParams)
    }

// TODO - not supported	
    public createBroadcast(String sourceAddress, BigDecimal feeFraction, String text, int timestamp, BigDecimal value) {	
		params = [sourceAddress, feeFraction, text, timestamp, value]
		return null
	}

//  TODO not supported
	// recall asset in mastercoin is actually an integer, though we use strings!
    public createIssuance(sourceAddress, asset, quantity, divisible, description) {
        myParams = [sourceAddress, Integer.parseInt(asset, 10) , quantity, divisible, description]
		return null
    }	
	
    public sendDividend(sourceAddress, total_quantity, asset, dividend_asset) {
		return sendRPCMessage('sendtoowners_MP',[sourceAddress, total_quantity,Integer.parseInt(dividend_asset, 10)])
	}
		
    public getAssetInfo(asset) {
		return sendRPCMessage('getproperty_MP',[Integer.parseInt(asset, 10)])
	}
	
	public getBurn() {
		return null
    }

    public MastercoinAPI(logger) {
        init()
		log4j = logger
    }

}
