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
	this->local_time = 0;
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

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	//log->LOG(&memberNode->addr, "update finish");
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(curMemList.size() != ring.size()){
		ring.clear();
		for(auto &n : curMemList)
			ring.push_back(n);
		stabilizationProtocol();
	}
	else{
		bool need = false;
		for(auto i1 = ring.begin(), i2 = curMemList.begin(); i1 != ring.end(); i1++, i2++){
			if((*i1).nodeAddress.getAddress() != (*i2).nodeAddress.getAddress()){
				need = true;
				break;
			}
		}
		if(need){
			ring.clear();
			for(auto &n : curMemList)
				ring.push_back(n);
			stabilizationProtocol();
		}
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
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */

	//log->LOG(&memberNode->addr, "Create start %s", key.c_str());

	vector<Node> memList = findNodes(key);
	if(memList.empty()){
		//log->LOG(&memberNode->addr, "No nodes");
	}
	Node n1 = memList[0], n2 = memList[1], n3 = memList[2];
	int cur_transID = g_transID++;


	//log->LOG(&memberNode->addr, "Addresses are %s, %s, %s", n1.nodeAddress.getAddress().c_str(), n2.nodeAddress.getAddress().c_str(), n3.nodeAddress.getAddress().c_str());
	//now send this message to the three processes
	Message* cur_msg = new Message(cur_transID, memberNode->addr, CREATE, key, value, PRIMARY);
	char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n1.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &(n1.nodeAddress), data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	cur_msg = new Message(cur_transID, memberNode->addr, CREATE, key, value, SECONDARY);
	data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n2.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &n2.nodeAddress, data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	cur_msg = new Message(cur_transID, memberNode->addr, CREATE, key, value, TERTIARY);
	data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n3.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &n3.nodeAddress, data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	wait_element* WE = new wait_element;
	WE->msgType = CREATE;
	WE->transID = cur_transID;
	WE->key = key;
	WE->value = value;
	WE->count = 0;
	WE->cur_time = local_time;
	WE->should_drop = false;

	waitingForReply.push_back(WE); 

	//log->LOG(&memberNode->addr, "Create end");

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
	//log->LOG(&memberNode->addr, "Read start");

	vector<Node> memList = findNodes(key);
	if(memList.empty()) return;
	int cur_transID = g_transID++;
	for(int i = 0; i < 3; i++){
		log->LOG(&memberNode->addr, "Choose %s for read", memList[i].nodeAddress.getAddress().c_str());
		Message* cur_msg = new Message(cur_transID, memberNode->addr, READ, key);
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = memList[i].nodeAddress;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(memList[i].nodeAddress), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}

	wait_element* WE = new wait_element;
	WE->msgType = READ;
	WE->transID = cur_transID;
	WE->key = key;
	WE->count = 0;
	WE->cur_time = local_time;
	WE->should_drop = false;

	waitingForReply.push_back(WE); 

	//log->LOG(&memberNode->addr, "Read end");

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
	//log->LOG(&memberNode->addr, "Update start");

	vector<Node> memList = findNodes(key);
	if(memList.empty()) return;
	Node n1 = memList[0], n2 = memList[1], n3 = memList[2];
	int cur_transID = g_transID++;
	//log->LOG(&memberNode->addr, "Addresses are %s, %s, %s", n1.nodeAddress.getAddress().c_str(), n2.nodeAddress.getAddress().c_str(), n3.nodeAddress.getAddress().c_str());

	//now send this message to the three processes
	Message* cur_msg = new Message(cur_transID, memberNode->addr, UPDATE, key, value, PRIMARY);
	char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n1.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &(n1.nodeAddress), data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	cur_msg = new Message(cur_transID, memberNode->addr, UPDATE, key, value, SECONDARY);
	data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n2.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &n2.nodeAddress, data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	cur_msg = new Message(cur_transID, memberNode->addr, UPDATE, key, value, TERTIARY);
	data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
	((en_msg*)data)->size = sizeof(Message);
	((en_msg*)data)->from = memberNode->addr;
	((en_msg*)data)->to = n3.nodeAddress;
	memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
	emulNet->ENsend(&memberNode->addr, &n3.nodeAddress, data, sizeof(en_msg) + sizeof(Message));
	delete cur_msg;
	free(data);

	wait_element* WE = new wait_element;
	WE->msgType = UPDATE;
	WE->transID = cur_transID;
	WE->key = key;
	WE->value = value;
	WE->count = 0;
	WE->cur_time = local_time;
	WE->should_drop = false;

	waitingForReply.push_back(WE); 

	//log->LOG(&memberNode->addr, "Update end");
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
	//log->LOG(&memberNode->addr, "Delete start %s", key.c_str());

	vector<Node> memList = findNodes(key);
	if(memList.empty()){
		log->LOG(&memberNode->addr, "No nodes");
		return;
	}
	int cur_transID = g_transID++;
	for(int i = 0; i < 3; i++){
		Message* cur_msg = new Message(cur_transID, memberNode->addr, DELETE, key);
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = memList[i].nodeAddress;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(memList[i].nodeAddress), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}

	wait_element* WE = new wait_element;
	WE->msgType = DELETE;
	WE->transID = cur_transID;
	WE->key = key;
	WE->count = 0;
	WE->cur_time = local_time;
	WE->should_drop = false;

	waitingForReply.push_back(WE); 

	//log->LOG(&memberNode->addr, "Delete end");
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key,value);
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
	return ht->read(key);
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
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
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
	//log->LOG(&memberNode->addr, "%d", local_time);
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		//log->LOG(&memberNode->addr, "One message");

		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		Message* recvMsg = (Message*)(data + sizeof(en_msg));
		//log->LOG(&memberNode->addr, "Got message from %s of type %d", recvMsg->fromAddr.getAddress().c_str(), recvMsg->type);
		switch(recvMsg->type){
			case CREATE:
				handleCreate(recvMsg);
				break;
			case READ:
				handleRead(recvMsg);
				break;
			case UPDATE:
				handleUpdate(recvMsg);
				break;
			case DELETE:
				handleDelete(recvMsg);
				break;
			case REPLY:
				handleReply(recvMsg);
				break;
			case READREPLY:
				handleReplyRead(recvMsg);
				break;
			default:
				break;
		}

		/*
		 * Handle the message types here
		 */

	}
	//log->LOG(&memberNode->addr, "checkMessages finish");

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

/************************************
 * Cleans up stale entries in wait  *
 * list.                            *
 ************************************/
void MP2Node::cleanUpWait(){
	auto i = waitingForReply.begin();
	while(i != waitingForReply.end()){
		wait_element* WE = *i;
		if(local_time - WE->cur_time > WAIT_TIME){
			switch(WE->msgType){
				case CREATE:
					log->logCreateFail(&memberNode->addr, true, WE->transID, WE->key, WE->value);
					break;
				case READ:
					log->logReadFail(&memberNode->addr, true, WE->transID, WE->key);
					break;
				case UPDATE:
					log->logUpdateFail(&memberNode->addr, true, WE->transID, WE->key, WE->value);
					break;
				case DELETE:
					log->logDeleteFail(&memberNode->addr, true, WE->transID, WE->key);
					break;
				default:
					i++;
					break;
			}
			waitingForReply.erase(i);
			delete(WE);
		}
		else i++;
	}
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
    	this->local_time++;
    	//log->LOG(&memberNode->addr, "Past");
    	cleanUpWait();
  		//log->LOG(&memberNode->addr, "recvloop finish");
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

void MP2Node::handleReply(Message* msg){
	//log->LOG(&memberNode->addr, "Found position");
	//log->LOG(&memberNode->addr, "HRe+");
	int transID = msg->transID;
	auto pos = waitingForReply.begin();
	while(pos != waitingForReply.end()){
		if((*pos)->transID == transID) break;
		pos++;
	}
	if(pos == waitingForReply.end()){
		//log->LOG(&memberNode->addr, "HRe-");
		return;
	}
	if(msg->success == false){
		if((*pos)->should_drop){
			switch ((*pos)->msgType){
				case CREATE:
					log->logCreateFail(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
					break;
				case UPDATE:
					log->logUpdateFail(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
					break;
				case DELETE:
					log->logDeleteFail(&memberNode->addr, true, transID, (*pos)->key);
					break;
				default:
					break;
			}
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}
		else{
			(*pos)->should_drop = true;
			//If its a failed msg then dont incr count, only change drop.
		}
		//log->LOG(&memberNode->addr, "HRe-");
		return;
	}

	if((*pos)->count <= 0){
		(*pos)->count += 1;
		return;
	}
	// Else atleast 2 nodes have now replied
	switch ((*pos)->msgType){
		case CREATE:
			log->logCreateSuccess(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
			break;
		case UPDATE:
			log->logUpdateSuccess(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
			break;
		case DELETE:
			log->logDeleteSuccess(&memberNode->addr, true, transID, (*pos)->key);
			break;
		default:
			break;
	}

	//Now we can remove the entry from waitingForReply
	wait_element* tmp = *pos;
	waitingForReply.erase(pos);
	delete tmp;
	//log->LOG(&memberNode->addr, "HRe-");
}

void MP2Node::handleReplyRead(Message* msg){
	//log->LOG(&memberNode->addr, "HRR+");
	int transID = msg->transID;
	auto pos = waitingForReply.begin();
	while(pos != waitingForReply.end()){
		if((*pos)->transID == transID) break;
		pos++;
	}
	if(pos == waitingForReply.end()) return;
	//Now we are sure that the entry exists

	if(msg->success == false){
		if((*pos)->should_drop){
			log->logReadFail(&memberNode->addr, true, transID, (*pos)->key);
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}
		else{
			(*pos)->should_drop = true;
			//If its a failed msg then dont incr count, only change drop.
		}
		return;
	}

	if((*pos)->count == 0){
		(*pos)->count++;
		(*pos)->value = msg->value;
	}
	else if((*pos)->count == 1 && !((*pos)->should_drop)){
		(*pos)->count++;
		if((*pos)->value == msg->value){
			log->logReadSuccess(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}
		else{
			(*pos)->conflicting_value = msg->value;
		}
	}
	else{
		(*pos)->count++;
		if((*pos)->value == msg->value){
			log->logReadSuccess(&memberNode->addr, true, transID, (*pos)->key, (*pos)->value);
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}
		else if((*pos)->conflicting_value == msg->value){
			log->logReadSuccess(&memberNode->addr, true, transID, (*pos)->key, (*pos)->conflicting_value);
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}
		else{
			log->logReadFail(&memberNode->addr, true, transID, (*pos)->key);
			wait_element* tmp = *pos;
			waitingForReply.erase(pos);
			delete tmp;
		}

	}
	//log->LOG(&memberNode->addr, "HRR-");
	//Already an entry exists
	//If the key matches, accept it, else do nothing
}


void MP2Node::handleCreate(Message* msg){
	//log->LOG(&memberNode->addr, "HC+");
	if(readKey(msg->key) != "") return; //To handle duplicates
	if(msg->delimiter == "replica"){
		//log->LOG(&memberNode->addr, "Backing up %s : %s", msg->key.c_str(), msg->value.c_str());
		createKeyValue(msg->key, msg->value, msg->replica);
		return;
	}
	if(createKeyValue(msg->key, msg->value, msg->replica)){
		log->logCreateSuccess(&memberNode->addr, false, msg->transID, msg->key, msg->value);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key, msg->value, msg->replica);
		cur_msg->success = true;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	else{
		log->logCreateFail(&memberNode->addr, false, msg->transID, msg->key, msg->value);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key, msg->value, msg->replica);
		cur_msg->success = false;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	//log->LOG(&memberNode->addr, "HC-");
}

void MP2Node::handleRead(Message* msg){
	//log->LOG(&memberNode->addr, "HR+");
	string value = readKey(msg->key);
	if(value == ""){
		log->logReadFail(&memberNode->addr, false, msg->transID, msg->key);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, READREPLY, msg->key);
		cur_msg->success = false;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	else{
		log->logReadSuccess(&memberNode->addr, false, msg->transID, msg->key, value);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, READREPLY, msg->key, value, msg->replica);
		cur_msg->success = true;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	//log->LOG(&memberNode->addr, "HR-");
	
}

void MP2Node::handleUpdate(Message* msg){
	//log->LOG(&memberNode->addr, "HU+");
	if(updateKeyValue(msg->key, msg->value, msg->replica)){
		log->logUpdateSuccess(&memberNode->addr, false, msg->transID, msg->key, msg->value);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key, msg->value, msg->replica);
		cur_msg->success = true;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	else{
		log->logUpdateFail(&memberNode->addr, false, msg->transID, msg->key, msg->value);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key, msg->value, msg->replica);
		cur_msg->success = false;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	//log->LOG(&memberNode->addr, "HU-");
	
}

void MP2Node::handleDelete(Message* msg){
	//log->LOG(&memberNode->addr, "HD+");
	if(deletekey(msg->key)){
		log->logDeleteSuccess(&memberNode->addr, false, msg->transID, msg->key);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key);
		cur_msg->success = true;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	else{
		log->logDeleteFail(&memberNode->addr, false, msg->transID, msg->key);

		Message* cur_msg = new Message(msg->transID, memberNode->addr, REPLY, msg->key);
		cur_msg->success = false;
		char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
		((en_msg*)data)->size = sizeof(Message);
		((en_msg*)data)->from = memberNode->addr;
		((en_msg*)data)->to = msg->fromAddr;
		memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
		emulNet->ENsend(&memberNode->addr, &(msg->fromAddr), data, sizeof(en_msg) + sizeof(Message));
		delete cur_msg;
		free(data);
	}
	//log->LOG(&memberNode->addr, "HD-");
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
	//log->LOG(&memberNode->addr, "Stabilizing.");
	for(auto &elt : ht->hashTable){
		string key = elt.first;
		string value = elt.second;
		//log->LOG(&memberNode->addr, "Doing key %s : %s", key.c_str(), value.c_str());
		vector<Node> memList = findNodes(key);
		int cur_transID = g_transID++;
		for(auto &n : memList){
			//log->LOG(&memberNode->addr, "%s", n.nodeAddress.getAddress().c_str());
			Message* cur_msg = new Message(cur_transID, memberNode->addr, CREATE, key, value);
			cur_msg->delimiter = "replica";
			char* data = (char*) malloc(sizeof(en_msg) + sizeof(Message));
			((en_msg*)data)->size = sizeof(Message);
			((en_msg*)data)->from = memberNode->addr;
			((en_msg*)data)->to = n.nodeAddress;
			memcpy(data + sizeof(en_msg), cur_msg, sizeof(Message));
			emulNet->ENsend(&memberNode->addr, &(n.nodeAddress), data, sizeof(en_msg) + sizeof(Message));
			delete cur_msg;
			free(data);
			//No need to make waitlist entry.
		}
		
	}
}
