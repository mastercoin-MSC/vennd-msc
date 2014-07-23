/**
 * Created by amirza on 19/07/14.
 */

public class DBCreator {
	public static createDB(db) { 
	
		db.execute("create table if not exists blocks (blockId integer, status string, duration integer)")
        db.execute("create table if not exists transactions(blockId integer, txid string)")
		// Todo Handle credits
        db.execute("create table if not exists credits(blockIdSource integer, txid string, sourceAddress string, destinationAddress string, inAsset string, inAmount integer, outAsset string, outAmount integer, status string)")
        db.execute("create table if not exists inputAddresses(txid string, address string)")
        db.execute("create table if not exists outputAddresses(txid string, address string)")
        db.execute("create table if not exists fees(blockId string, txid string, feeAsset string, feeAmount integer)")
        db.execute("create table if not exists payments(blockId integer, sourceTxid string, sourceAddress string, inAssetType string, destinationAddress string, outAsset string, outAssetType string, outAmount integer, status string, lastUpdatedBlockId integer)")
		// TODO Handle issuances 
        db.execute("create table if not exists issuances(blockId integer, sourceTxid string, destinationAddress string, asset string, amount integer, divisibility string, status string, lastUpdatedBlockId integer)")
		db.execute("create table if not exists counterpartyBlocks (blockId integer, status string, duration integer)")
		db.execute("create table if not exists mastercoinBlocks (blockId integer, status string, duration integer)")

        db.execute("create unique index if not exists blocks1 on blocks(blockId)")
        db.execute("create index if not exists transactions1 on transactions(blockId)")
        db.execute("create index if not exists transactions2 on transactions(txid)")
        db.execute("create index if not exists credits1 on credits(blockIdSource)")
        db.execute("create index if not exists credits2 on credits(txid)")
        db.execute("create index if not exists fees1 on fees(blockId, txid)")
        db.execute("create index if not exists inputAddresses1 on inputAddresses(txid)")
        db.execute("create index if not exists inputAddresses2 on inputAddresses(address)")
        db.execute("create index if not exists outputAddresses1 on outputAddresses(txid)")
        db.execute("create index if not exists outputAddresses2 on outputAddresses(address)")
        db.execute("create index if not exists payments1 on payments(blockId)")
        db.execute("create index if not exists payments1 on payments(sourceTxid)")
        db.execute("create index if not exists issuances1 on issuances(blockId)")
		db.execute("create unique index if not exists counterpartyBlocks1 on counterpartyBlocks(blockId)")
        db.execute("create unique index if not exists mastercoinBlocks1 on mastercoinBlocks(blockId)")
		
		def row
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
        row = db.firstRow("select name from sqlite_master where type='table' and name='credits'")
        assert row != null
        row = db.firstRow("select name from sqlite_master where type='table' and name='counterpartyBlocks'")
        assert row != null
		row = db.firstRow("select name from sqlite_master where type='table' and name='mastercoinBlocks'")
        assert row != null
	}
}
