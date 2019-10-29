/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define QUORUM_NEEDED 		2;
#define QUORUM_OBTAINED_SUCCESS		1;
#define QUORUM_OBTAINED_FAILURE		3;
//#define MESSAGE_SUCESSFULL 	1;
//#define MESSAGE_FAILED 		2;
//#define MESSAGE_STATUS_PENDING 3;

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	bool coordinator;
	MessageType myLastMsg;
	HashTable * posQuorum;
	HashTable * negQuorum;
	HashTable * readQuorum;

	string replyRead;
	bool initialRingSetup;
	bool ringHasChanged;
	int ringSize;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int transID);
	string readKey(string key, int transID);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key, int transID);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();
	// MY ADDED FUCTION //
	int checkCreateReply(int transID, string qCount, string replyKey, string replyData, bool msgSuccessful);
	int checkDeleteReply(int transID, bool msgSuccessful);
	void checkForFailedReply();
	int getNodeRingPosition();
	void checkForQuorum();

};

#endif /* MP2NODE_H_ */
