/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
//msgCounter = 1;
	coordinator = false;
	posQuorum = new HashTable();
	negQuorum = new HashTable();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();
//	cout << "first item of memList " << curMemList.front << endl;
	/*
	 * Step 2: Construct the ring
	 */

	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {		// treat this memberNode as coordinator
	/*
	 * Implement this
	 */

	// 1) construct the messages
	int msgID = rand() % 2000;
	Message *newMsgPrimary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, PRIMARY);
	Message *newMsgSecondary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, SECONDARY);
	Message *newMsgTertiary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, TERTIARY);

	// ***  this may need some work ****//+
//	log->logCreateSuccess(&memberNode->addr, false, msgID, key, value);


	// 2) find the replicas of key
	vector<Node> nodeReplicaList = findNodes(key);
	
	// 3) sends message to the replicas (not yet being rec'd and processed)
	// guessing the server side needs the message to write "value" to each key
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), newMsgPrimary->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), newMsgSecondary->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), newMsgTertiary->toString());
	string delim = " ";
	string idData = "0" + delim + key + delim + value;
//	cout << htData << endl;
	posQuorum->create(to_string(msgID), idData);
	negQuorum->create(to_string(msgID), idData);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	// step 1 - create the message

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */

	vector<Node> nodeReplicaList = findNodes(key);
	int msgID = rand() % 2000 + 2001;  // random number from 2001 and 4000 
	Message *deleteMsg = new Message(msgID, memberNode->addr.getAddress(), DELETE, key);

	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), deleteMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), deleteMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), deleteMsg->toString());

	string delim = " ";
	string idData = "0" + delim + key + delim + "EMPTY";

	posQuorum->create(to_string(msgID), idData);
	negQuorum->create(to_string(msgID), idData);
	
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	if(ht->create(key,value))
	{
		log->logCreateSuccess(&memberNode->addr, coordinator, transID, key, value);
		return true;
	}
	else
	{
		log->logCreateFail(&memberNode->addr, coordinator, transID, key, value);
		return false;
	}
	
//	cout << " added to log at key " << key << endl;
	
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if(ht->deleteKey(key))
	{
		log->logDeleteSuccess(&memberNode->addr, coordinator, transID, key);
		return true;
	}
	else
		return false;

//	deleteCount++;
//	cout << " delete count " << deleteCount << endl;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		Message receivedMessage(message);

		MessageType msgType = receivedMessage.type;
		Address fromAddress = receivedMessage.fromAddr;
		int transID = receivedMessage.transID;
		string sTransID = to_string(transID);

		string key = receivedMessage.key;
		string value = receivedMessage.value;
		ReplicaType type = receivedMessage.replica;			// may not need this

		string qCount;		// current quorum count
		
		Message *replyMsg;

		bool requestSucessfull = false;
	
		if(fromAddress == memberNode->addr)
		{
		//	cout << " this is the corrdinator. send address: " << fromAddress.addr << " member address " << memberNode->addr.addr[0] << endl;
			coordinator = true;
		}
		else
		{
			coordinator = false;
		}
		
		/*
		 * Handle the message types here
		 */
		// need to create 3 copies of it
		switch(msgType)
    	{
        	case(CREATE):	
				
				requestSucessfull = createKeyValue(key, value, type, transID);
				replyMsg = new Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
				emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				requestSucessfull = false;
			break;

			case(DELETE):
				requestSucessfull = deletekey(key, transID);
				if(requestSucessfull)
				{
					replyMsg = new Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
					emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				}
			break;

			case(REPLY):	// let coordinator know if request was sucessfull
			//Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
			string transID_Data;
			string replyKey, replyData;
			int replyStatus = 0;
			bool msgSuccessful = receivedMessage.success;
			transID_Data = posQuorum->read(sTransID);

			if(transID_Data.length() < 1)
				break;
			
			vector <string> tokens;
			std::stringstream streamData(transID_Data);
			string tempData;

			while(getline(streamData, tempData, ' '))
			{
				tokens.push_back(tempData);
			}
			qCount = tokens[0];
			replyKey = tokens[1];
			replyData = tokens[2];

		//	cout << " q count " << qCount << " key " << replyKey << " value " << replyData << endl;

			if(qCount.length() > 0)
			{
				if(transID >= 0 && transID < 2001)			// create message
				{
					replyStatus = checkCreateReply(transID, qCount, replyKey, replyData, msgSuccessful);
					if(replyStatus == 1)
					{
						log->logCreateSuccess(&memberNode->addr, coordinator, transID, replyKey, replyData);	
						posQuorum->deleteKey(sTransID);	
					}
				}
				if(transID > 2000 && transID < 4001)		// delete message
				{
					replyStatus = checkCreateReply(transID, qCount, replyKey, replyData, msgSuccessful);
					if(replyStatus == 1)
					{
						log->logDeleteSuccess(&memberNode->addr, coordinator, transID, replyKey);
						posQuorum->deleteKey(sTransID);	
					}
				}

			}
			break;
		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}

// ******************* MY ADDED FUNCTIONS ******************** //

// checks if reply message satisfies quorum
int MP2Node::checkCreateReply(int transID, string qCount, string replyKey, string replyData, bool msgSuccessful)
{
	int iQCount;
	string sTransID = to_string(transID);
	string delim = " ";
	if(msgSuccessful)
	{
	//qCount = posQuorum->read(sTransID);
		iQCount = stoi(qCount);
		if(iQCount == 0)
			posQuorum->update(sTransID, "1" + delim + replyKey + delim + replyData);
		if(iQCount == 1)
			posQuorum->update(sTransID, "2"+ delim + replyKey + delim + replyData);
		if(iQCount == 2)
			posQuorum->update(sTransID, "3"+ delim + replyKey + delim + replyData);

		iQCount = stoi(posQuorum->read(sTransID));

		if(iQCount == 2)
		{
			return 1;
			//log->logCreateSuccess(&memberNode->addr, coordinator, transID, key, value);	
			//posQuorum->deleteKey(sTransID);
		}
	}

	if(!msgSuccessful)
	{
		iQCount = stoi(negQuorum->read(sTransID));
		if(iQCount == 0)
			negQuorum->update(sTransID, "1");
		if(iQCount == 1)
				negQuorum->update(sTransID, "2");
		if(iQCount == 2)
				negQuorum->update(sTransID, "3");

		iQCount = stoi(negQuorum->read(sTransID));

		if(iQCount == 2)
		{
			return 2;
			//log->logCreateFail(&memberNode->addr, coordinator, transID, key, value);
			//negQuorum->deleteKey(sTransID);	
		}
	}
	return 0;
}
void MP2Node::checkForQuorum()
{

}