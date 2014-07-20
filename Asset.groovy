/**
 * Created by amirza on 19/07/14.
 */

public class Asset {
	static String MASTERCOIN_TYPE = "MSC"
	static String COUNTERPARTY_TYPE = "XCP"
	static String NATIVE_TYPE = "BTC"

    def String counterpartyAssetName
	def String mastercoinAssetName
    def String nativeAssetName
	
    def String counterpartyAddress // the counterparty/bitcoin address side where we will receive the Counterparty asset
	def String mastercoinAddress // the mastercoin/bitcoin address side where we will receive the mastercoin asset
    def String nativeAddressCounterparty // The address which users should send their native asset to get counterparty
	def String nativeAddressMastercoin // The address which users should send their native asset to get mastercoin
	def String counterpartyToMastercoinAddress // The address for a counterparty -> mastercoin conversion
	def String mastercoinToCounterpartyAddress // The address for a mastercoin -> counterparty conversion

    def BigDecimal txFee
    def BigDecimal feePercentage
    def boolean mappingRequired
    def issuanceDependent
    def issuanceSource
    def issuanceAsset
    def issuanceDivisible
    def issuanceDescription

    public Asset(String counterpartyAssetNameValue, String mastercoinAssetName, String nativeAssetNameValue, String counterpartyAddressValue, String mastercoinAddressValue, String nativeAddressCounterpartyValue, String nativeAddressMastercoinValue, String counterpartyToMastercoinAddressValue, String mastercoinToCounterpartyAddressValue,BigDecimal txFeeValue, BigDecimal feePercentageValue, boolean mappingRequiredValue, boolean issuanceDependentValue, String issuanceSourceValue, String issuanceAssetValue, boolean issuanceDivisibleValue, String issuanceDescriptionValue) {
       
	    counterpartyAssetName = counterpartyAssetNameValue
		mastercoinAssetName  = mastercoinAssetNameValue
		nativeAssetName = nativeAssetNameValue
		
        counterpartyAddress = counterpartyAddressValue
		mastercoinAddress = mastercoinAddressValue
		nativeAddressCounterparty = nativeAddressCounterpartyValue
		nativeAddressMastercoin = nativeAddressMastercoinValue
		counterpartyToMastercoinAddress = counterpartyToMastercoinAddressValue
		mastercoinToCounterpartyAddress = mastercoinToCounterpartyAddressValue
		
        nativeAddress = nativeAddressValue
        txFee = txFeeValue
        feePercentage = feePercentageValue
        mappingRequired = mappingRequiredValue
        issuanceDependent = issuanceDependentValue
        issuanceSource = issuanceSourceValue
        issuanceAsset = issuanceAssetValue
        issuanceDivisible = issuanceDivisibleValue
        issuanceDescription = issuanceDescriptionValue
    }
	
	public static readAssets(String file) {
		iniConfig = new ConfigSlurper().parse(new File(file).toURL())
		
		assetConfig = []
        iniConfig.asset.each { it ->
            def issuanceDependent
            def issuanceSource
            def issuanceAsset
            def issuanceDivisible
            def issuanceDescription

            if (it.value.issuanceDependent instanceof groovy.util.ConfigObject) { issuanceDependent = false} else {issuanceDependent = it.value.issuanceDependent}
            if (it.value.issuanceSource instanceof groovy.util.ConfigObject) { issuanceSource = ""} else {issuanceSource = it.value.issuanceSource}
            if (it.value.issuanceAsset instanceof groovy.util.ConfigObject) { issuanceAsset = ""} else {issuanceAsset = it.value.issuanceAsset}
            if (it.value.issuanceDivisible instanceof groovy.util.ConfigObject) { issuanceDivisible = true} else {issuanceDivisible = it.value.issuanceDivisible}
            if (it.value.issuanceDescription instanceof groovy.util.ConfigObject) { issuanceDescription = ""} else {issuanceDescription = it.value.issuanceDescription}

            def currentAsset = new Asset(it.value.counterpartyAssetName, it.value.mastercoinAssetName, it.value.nativeAssetName, it.value.counterpartyAddress, it.value.mastercoinAddress, it.value.nativeAddressCountrparty , it.value.nativeAddressMastercoin, it.value.counterpartyToMastercoinAddress,it.value.mastercoinToCounterpartyAddress, it.value.txFee, it.value.feePercentage, it.value.mappingRequired, issuanceDependent, issuanceSource, issuanceAsset, issuanceDivisible, issuanceDescription)
            assetConfig.add(currentAsset)
        }
		
		return assetConfog
	}
}