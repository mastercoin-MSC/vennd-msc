/**
 * Created by whoisjeremylam on 18/04/14.
 * All amounts are in floating point, not willets
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
                if (json.containsKey("error") && json.error != null) {
                    log4j.info(command + " error: " + json.error)
                    return json.error
                }
				
                return json.result
            }

 	    response.failure = { resp -> 
			log4j.info(command + " failed") 
			assert resp.responseBase == null
	    }

        }

        assert result instanceof java.util.concurrent.Future
        while ( ! result.done ) {
            Thread.sleep(100)
        }
			
        return result.get()
	}

    public getBalances(address) {    
		return sendRPCMessage('getallbalancesforaddress_MP', [address])
    }
	
	public getAssetBalance(address, asset) {
		return sendRPCMessage('getbalance_MP', [address, Long.parseLong(asset, 10)])
	}

    public getSends(Long blockId) {
		return sendRPCMessage('listtransactions_MP', ["*", 10000, 0, blockId,blockId])

	}

	// Recall asset is integer in mastercoin (amount in floating point, not willet)
    public sendAsset(sourceAddress, destinationAddress, asset, amount, testMode) {
        def myParams

        if (testMode == false) {
            myParams = [sourceAddress, destinationAddress, Long.parseLong(asset, 10), amount]
        }
        else {
			myParams = ['12nY87y6qf4Efw5WZaTwgGeceXApRYAwC7', '142UYTzD1PLBcSsww7JxKLck871zRYG5D3', Long.parseLong(asset, 10), 20000]
        }		
		return sendRPCMessage('send_MP', myParams)
    }
	
    // Amount is in floating point, not willets
    public sendDividend(String sourceAddress, String dividend_asset, BigDecimal total_quantity) {
		return sendRPCMessage('sendtoowners_MP',[sourceAddress, Long.parseLong(dividend_asset, 10), total_quantity])
	}
		
    public getAssetInfo(asset) {
		return sendRPCMessage('getproperty_MP',[Long.parseLong(asset, 10)])
	}
	
    public MastercoinAPI(logger) {
        init()
		log4j = logger
    }

}
