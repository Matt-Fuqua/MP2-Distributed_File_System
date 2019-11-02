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

	coordinator = false;
	initialRingSetup = true;
	ringHasChanged = false;
	ringSize = 0;
	posQuorum = new HashTable();
	negQuorum = new HashTable();
	readQuorum = new HashTable();	
	updateQuorum = new HashTable();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	delete readQuorum;
	delete posQuorum;
	delete negQuorum;
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
	
	vector<Node> curMemList;

	// Step 1. Get the current membership list from Membership Protocol / MP1
	curMemList = getMembershipList();

	//Step 2: Construct the ring
	sort(curMemList.begin(), curMemList.end());		// sort ring based on hashCode
	ring = curMemList;

	if(initialRingSetup == true)
	{
		int pos = getNodeRingPosition();
		hasMyReplicas.emplace_back(ring.at((pos+1)%ring.size()));
		hasMyReplicas.emplace_back(ring.at((pos+2)%ring.size()));
		ringSize = ring.size();
		initialRingSetup = false;
	}

	// Step 3: Run the stabilization protocol IF REQUIRED
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	// check to see if the size has changed since last ringsetup

	if(ringSize != ring.size())	// change in number of nodes in the ring. need to engadge the stabilizers!
	{
		stabilizationProtocol();
	}
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

	// 1) construct the messages
	int msgID = rand() % 2000;
	Message *newMsgPrimary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, PRIMARY);

	// 2) find the replicas of key
	vector<Node> nodeReplicaList = findNodes(key);
	
	// 3) sends message to the replicas 
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), newMsgPrimary->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), newMsgPrimary->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), newMsgPrimary->toString());

	// format: quorumCount key data time
	string delim = " ";
	string idData = "0" + delim + key + delim + value + delim + to_string(par->getcurrtime());
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

	// step 1 - create the message
	int msgID = rand() % 2000 + 4001;  // random number from 4001 and 6000

	Message *newReadMsg = new Message(msgID, memberNode->addr.getAddress(), READ, key); 

	// 2) find the replicas of key
	vector<Node> nodeReplicaList = findNodes(key);

	// 3) sends message to the replicas 
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), newReadMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), newReadMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), newReadMsg->toString());

	string delim = " ";
	long msgTime = par->getcurrtime();
	string idData = key + delim + "EMPTY" + delim + to_string(msgTime);
	
	readQuorum->create(to_string(msgID), idData);
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

	vector<Node> nodeReplicaList = findNodes(key);
	int msgID = rand() % 2000 + 6001;  // random number from 6001 and 8000 
	Message *updateMsg = new Message(msgID, memberNode->addr.getAddress(), UPDATE, key, value);
	
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), updateMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), updateMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), updateMsg->toString());

	// format: quorumCount key value time
	string delim = " ";
	string idData = "0" + delim + key + delim + value + delim + to_string(par->getcurrtime());

	posQuorum->create(to_string(msgID), idData);
	negQuorum->create(to_string(msgID), idData);
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
	int msgID = rand() % 2000 + 2001;  // random number from 4001 and 6000 
	Message *deleteMsg = new Message(msgID, memberNode->addr.getAddress(), DELETE, key);

	emulNet->ENsend(&memberNode->addr, nodeReplicaList[0].getAddress(), deleteMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[1].getAddress(), deleteMsg->toString());
	emulNet->ENsend(&memberNode->addr, nodeReplicaList[2].getAddress(), deleteMsg->toString());

	// format: quorumCount key value time
	string delim = " ";
	string idData = "0" + delim + key + delim + "EMPTY" + delim + to_string(par->getcurrtime());

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
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID) {

	string result;
	result = ht->read(key);

	return result;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if(ht->update(key, value))
	{
		log->logUpdateSuccess(&memberNode->addr, coordinator, transID, key, value);
		return true;
	}
	else
	{
		log->logUpdateFail(&memberNode->addr, coordinator, transID, key, value);
		return false;
	}
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
	{
		log->logDeleteFail(&memberNode->addr, coordinator, transID, key);
		return false;
	}
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
		string readResult;	// results from read request
		coordinator = false;

		Message *replyMsg;

		bool requestSucessfull = false;

		switch(msgType)
    	{
        	case(CREATE):
			{					
				requestSucessfull = createKeyValue(key, value, type, transID);
				if(type == PRIMARY)
				{
					replyMsg = new Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
					emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				}
				requestSucessfull = false;
				break;
			}

			case(DELETE):
			{
				requestSucessfull = deletekey(key, transID);
				replyMsg = new Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
				emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				break;
			}

			case(READ):
			{
				readResult = readKey(key, transID);

				if(readResult.length() > 0)
				{
					log->logReadSuccess(&memberNode->addr, coordinator, transID, key, readResult);	
					replyMsg = new Message(transID, memberNode->addr.getAddress(), READREPLY, key, readResult);
					emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				}
				else
					log->logReadFail(&memberNode->addr, coordinator, transID, key);
				
				break;
			}
			case(UPDATE):
			{
				requestSucessfull = updateKeyValue(key, value, type, transID);
				replyMsg = new Message(transID, memberNode->addr.getAddress(), REPLY, requestSucessfull);
				emulNet->ENsend(&memberNode->addr, &fromAddress, replyMsg->toString());
				break;
			}

			case(READREPLY):	
			{
				coordinator = true;
				string transID_Data;
				string tempData;
				string tempID, tempKey, tempValue, msgTime;
				string delim = " ";

				transID_Data = readQuorum->read(sTransID);	// get the data for this transID
				if(transID_Data.length() < 1)
					break;
	
				vector <string> tokens;
				std::stringstream streamData(transID_Data);
				while(getline(streamData, tempData, ' '))
				{
					tokens.push_back(tempData);
				}
				tempKey = tokens[0];
				tempValue = tokens[1];
				msgTime = tokens[2];
			
				int iMsgTime = stoi(msgTime);

				if (tempValue == "EMPTY")
				{
					string newValue = tempKey + delim + value + delim + msgTime;
					readQuorum->update(sTransID, newValue);
				}
				else
				{
					if(tempValue == value)			// second read reply. quorum reached
					{
						log->logReadSuccess(&memberNode->addr, coordinator, transID, tempKey, value);
						readQuorum->deleteKey(sTransID);
					}
				}
//				if(iMsgTime + 5 < par->getcurrtime())
//				{
//					cout << " reply msg not rec'd for transID " << transID << endl;
//					log->logReadSuccess(&memberNode->addr, coordinator, transID, tempKey, value);
//					readQuorum->deleteKey(sTransID);
//				}
			break;
			}

			case(REPLY):	// used for create / delete / update. NOT USED DURING STABILIZATION
			{
				coordinator = true;
				string transID_Data;
				string replyKey, replyData, replyTime;
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
				replyTime = tokens[3];

				if(qCount.length() > 0)
				{
					if(transID >= 0 && transID < 2001)			// create key
					{
						replyStatus = checkCreateReply(transID, qCount, replyKey, replyData, msgSuccessful, replyTime);
						if(replyStatus == 1)
						{
							log->logCreateSuccess(&memberNode->addr, coordinator, transID, replyKey, replyData);	
							posQuorum->deleteKey(sTransID);	
						}
					}
					if(transID > 2000 && transID < 4001)		// delete key %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
					{
						replyStatus = checkCreateReply(transID, qCount, replyKey, replyData, msgSuccessful, replyTime);
						if(replyStatus == 1)
						{
							log->logDeleteSuccess(&memberNode->addr, coordinator, transID, replyKey);
							posQuorum->deleteKey(sTransID);	
						}
						if(replyStatus == 3)
						{
							log->logDeleteFail(&memberNode->addr, coordinator, transID, replyKey);
							posQuorum->deleteKey(sTransID);	
						}
					}
					if(transID > 6000 && transID < 8001)		// update Key
					{
						replyStatus = checkCreateReply(transID, qCount, replyKey, replyData, msgSuccessful, replyTime);
						if(replyStatus == 1)
						{
							log->logUpdateSuccess(&memberNode->addr, coordinator, transID, replyKey, replyData);
							posQuorum->deleteKey(sTransID);	
						}
						if(replyStatus == 3)
						{
							log->logUpdateFail(&memberNode->addr, coordinator, transID, replyKey, replyData);
							posQuorum->deleteKey(sTransID);	
						}
					}
				}
				break;
			}
		}
	}
	checkForFailedReply();
	if(posQuorum->hashTable.size() > 0)		
		checkForFailedUpdateReply();		// *************** this is failing read test
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

void MP2Node::stabilizationProtocol() 
{
	ringSize = ring.size();
	int myRingPosition = getNodeRingPosition();
	int myAddress = *(int *)(&memberNode->addr.addr);

//	cout << "Engadging stabilizers captain! Time is  " << par->getcurrtime() << " ring size " << ringSize << endl;
//	cout << "my address " << myAddress << " time " << par->getcurrtime() << endl;

	Node expectedFirstNeighbor = hasMyReplicas.at(0);
	Node expectedSecondNeighbor = hasMyReplicas.at(1);
	int firstExpectedAddress = *(int *)(expectedFirstNeighbor.getAddress());
	int secondExpectedAddress = *(int *)(expectedSecondNeighbor.getAddress());

	Node actualFirstNeighbor = ring.at((myRingPosition+1)%ring.size());
	Node actualSecondNeighbor = ring.at((myRingPosition+2)%ring.size());
	int firstActualAddress = *(int *)(actualFirstNeighbor.getAddress());
	int secondActualAddress = *(int *)(actualSecondNeighbor.getAddress());

	int firstNeighborPosition = (myRingPosition+1)%ring.size();
	int secondNeighborPosition = (myRingPosition+2)%ring.size();

	
	if(firstExpectedAddress != firstActualAddress)				// first neighbor has failed
	{
		if(firstActualAddress == secondExpectedAddress)			// second neighbor shifted into first neighbor
		{
			cout << "first neighbor failed. second neighbor is now my first " << endl << endl;
			createOneReplica(secondNeighborPosition);
			hasMyReplicas.clear();		
			hasMyReplicas.emplace_back(ring.at((myRingPosition+1)%ring.size()));
			hasMyReplicas.emplace_back(ring.at((myRingPosition+2)%ring.size()));
		}
		if(firstActualAddress != secondExpectedAddress)			// both first and second neighbor failed
		{
			cout << "both neighbors have failed " << endl << endl;
			createTwoReplica(firstNeighborPosition, secondNeighborPosition);
			hasMyReplicas.clear();		
			hasMyReplicas.emplace_back(ring.at((myRingPosition+1)%ring.size()));
			hasMyReplicas.emplace_back(ring.at((myRingPosition+2)%ring.size()));	
		}
	}

	if(firstExpectedAddress == firstActualAddress)			// first neighbors is unchanged
	{
		if(secondExpectedAddress != secondActualAddress)	// second neighbor has failed
		{
			cout << "second neighbor failed " << endl << endl;
			createOneReplica(secondNeighborPosition);
			hasMyReplicas.clear();		
			hasMyReplicas.emplace_back(ring.at((myRingPosition+1)%ring.size()));
			hasMyReplicas.emplace_back(ring.at((myRingPosition+2)%ring.size()));
		}
	}

//	if((firstExpectedAddress == firstActualAddress) && secondExpectedAddress == secondActualAddress)
//	{
//		cout << "no changes needed " << endl << endl;
//	}
}

// ******************* MY ADDED FUNCTIONS ******************** //

// checks if reply message satisfies quorum
int MP2Node::checkCreateReply(int transID, string qCount, string replyKey, string replyData, bool msgSuccessful, string replyTime)
{
//	if(transID > 4000)
//	{
//		cout << "read check with key " << replyKey << " value " << replyData << endl;

//	}

	int iQCount;
	string sTransID = to_string(transID);
	string delim = " ";
	if(msgSuccessful)
	{
		// format: quorumCount key value time
		iQCount = stoi(qCount);
		if(iQCount == 0)
			posQuorum->update(sTransID, "1" + delim + replyKey + delim + replyData + delim + replyTime);
		if(iQCount == 1)
			posQuorum->update(sTransID, "2"+ delim + replyKey + delim + replyData + delim + replyTime);
		if(iQCount == 2)
			posQuorum->update(sTransID, "3"+ delim + replyKey + delim + replyData + delim + replyTime);

		iQCount = stoi(posQuorum->read(sTransID));

		if(iQCount == 2)
		{
			return QUORUM_OBTAINED_SUCCESS;
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
			return QUORUM_OBTAINED_FAILURE;
		}
	}
	return 0;
}

void MP2Node::checkForFailedReply()
{	
	coordinator = true;
	string checkTransID;
	string checkData;

	string tempData;
	string tempID, tempKey, tempValue, msgTime;
	string delim = " ";

	for (map<string,string>::iterator it =readQuorum->hashTable.begin(); it!=readQuorum->hashTable.end(); ++it)
	{
		checkTransID = it->first;
		checkData = it->second;
		vector <string> tokens;
		std::stringstream streamData(checkData);
	
		while(getline(streamData, tempData, ' '))
		{
			tokens.push_back(tempData);
		}
		tempKey = tokens[0];
		tempValue = tokens[1];
		msgTime = tokens[2];

		if((stoi(msgTime) + 10) < par->getcurrtime())
		{
			log->logReadFail(&memberNode->addr, coordinator, stoi(checkTransID), tempKey);
			readQuorum->deleteKey(checkTransID);
			
		}
	}
}
void MP2Node::checkForFailedUpdateReply()
{	
		
	coordinator = true;
	string checkTransID;
	string checkData;

	string tempData;
	string tempID, tempKey, tempValue, msgTime;
	string delim = " ";

	for (map<string,string>::iterator it =posQuorum->hashTable.begin(); it!=posQuorum->hashTable.end(); ++it)
	{
		checkTransID = it->first;
		checkData = it->second;
		vector <string> tokens;
		std::stringstream streamData(checkData);
		// format: quorumCount key value time
		if(stoi(checkTransID) > 6000 && stoi(checkTransID) < 8001)		// update Key
		{
			while(getline(streamData, tempData, ' '))
			{
				tokens.push_back(tempData);
			}
			tempKey = tokens[1];
			tempValue = tokens[2];
			msgTime = tokens[3];
			if((stoi(msgTime) + 10) < par->getcurrtime())
			{
				log->logUpdateFail(&memberNode->addr, coordinator, stoi(checkTransID), tempKey, tempValue);
				posQuorum->deleteKey(checkTransID);
				
			}
		}
	}
}


int MP2Node::getNodeRingPosition()
{
	int myRingPosition = -1;
	int nodeID = *(int *)(&memberNode->addr.addr); 
	Node tempNode;
	int ringAddress;

	// identify my position in the ring
	for(int i=1; i<ring.size(); i++)
	{	
		tempNode = ring.at(i);
		ringAddress = *(int *)(tempNode.getAddress());
		
		if(nodeID == ringAddress)
		{
			myRingPosition = i;
		}
	}
	return myRingPosition;
}

bool MP2Node::createOneReplica(int pos1)
{
	string key, value;
	int msgID = rand() % 2000;
	for (map<string,string>::iterator it = ht->hashTable.begin(); it!=ht->hashTable.end(); ++it)
	{	
		key = it->first;
		value = it->second;
		Message *newMsgSecondary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, SECONDARY);
		emulNet->ENsend(&memberNode->addr, ring.at(pos1).getAddress(), newMsgSecondary->toString());
	}
}

bool MP2Node::createTwoReplica(int pos1, int pos2)
{
	string key, value;
	int msgID = rand() % 2000;

	for (map<string,string>::iterator it = ht->hashTable.begin(); it!=ht->hashTable.end(); ++it)
	{	
		key = it->first;
		value = it->second;
		Message *newMsgSecondary = new Message(msgID, memberNode->addr.getAddress(), CREATE, key, value, SECONDARY);
		emulNet->ENsend(&memberNode->addr, ring.at(pos1).getAddress(), newMsgSecondary->toString());
		emulNet->ENsend(&memberNode->addr, ring.at(pos2).getAddress(), newMsgSecondary->toString());
	}
}
void MP2Node::checkForQuorum()
{

}