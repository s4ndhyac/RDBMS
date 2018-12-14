#include "rm_test_util.h"

RC RM_TEST_PRIVATE_EXTRA_1(const string &tableName)
{
	// Extra Test Case - Functions Tested:
    // 1. Insert tuple
    // 2. Read Attributes																																																				
    // 3. Drop Attributes **
    cout << endl << "***** In RM Private Extra Credit Test Case 1 *****" << endl;

    RID rid;
    RID rid2;
    int tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

	int tweetid = 10;
	int userid = 12;
	float sender_location = 98.99;
	float send_time = 10.11;
	string referred_topics = "private1";
	int referred_topics_length = 8;
	string message_text = "PassOrFail";
	int message_text_length = 10;		
		
    // Insert Tuple
    vector<Attribute> attrs;
    vector<Attribute> newAttrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

	RM_ScanIterator rmsi;
    string attr = "referred_topics";
    string attr2 = "message_text";
    vector<string> attributes;
    attributes.push_back(attr2);
    
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    prepareTweetTuple(attrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time, referred_topics_length, referred_topics, message_text_length, message_text, tuple, &tupleSize);
    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Read Attribute
    rc = rm->readAttribute(tableName, rid, "referred_topics", returnedData);
    assert(rc == success && "RelationManager::readAttribute() should not fail.");

	// length of string (4) + actual string (8) = 12. 12 bytes should be the same
    if(memcmp((char *)returnedData+nullAttributesIndicatorActualSize, (char *)tuple+16+nullAttributesIndicatorActualSize, 12) != 0)
    {
        cout << "***** RelationManager::readAttribute() failed. Private Extra Credit Test Case 1 Failed. *****" << endl;
        cout << endl;
        free(returnedData);
        free(tuple);
        return -1;
    }
    else
    {
    	// read Tuple again
        rc = rm->readTuple(tableName, rid, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");
    	
		// Print the tuple
		cout << "Before dropping an attribute:" << endl;
        rc = rm->printTuple(attrs, returnedData);

        // Drop the attribute
        rc = rm->dropAttribute(tableName, attr);
        assert(rc == success && "RelationManager::dropAttribute() should not fail.");

        // Read Tuple and print the tuple
        rc = rm->readTuple(tableName, rid, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");

        // Remove "referred_topics" attribute from the Attribute vector
        rc = rm->getAttributes(tableName, newAttrs);
        if (newAttrs.size() != attrs.size() - 1) {
			cout << "The number of attributes after dropping an attribute is not correct." << endl;
			cout << "***** [FAIL] Private Extra Credit Test Case 1 Failed. *****" << endl << endl;
			free(returnedData);
			free(tuple);
			return -1;
        }
		cout << endl << "After dropping an attribute:" << endl;
        rc = rm->printTuple(newAttrs, returnedData);
        assert(rc == success && "RelationManager::printTuple() should not fail.");

		// Insert one more tuple
		tweetid = 77;
		userid = 77;
		sender_location = 77.77;
		send_time = 77.78;
		message_text = "PassOrFail2";
		message_text_length = 11;	
	
    	prepareTweetTupleAfterDrop(attrs.size(), nullsIndicator, tweetid, userid, sender_location, send_time, message_text_length, message_text, tuple, &tupleSize);
		rc = rm->insertTuple(tableName, tuple, rid2);
		
        // Read new tuple and print the tuple
        rc = rm->readTuple(tableName, rid2, returnedData);
        assert(rc == success && "RelationManager::readTuple() should not fail.");
		
		cout << endl << "New tuple:" << endl;
        rc = rm->printTuple(newAttrs, returnedData);
        assert(rc == success && "RelationManager::printTuple() should not fail.");
		
    }
    
    // Scan() on message_text - should succeed
    // We scan this variable since it is placed after the "referred_topics" attribute.
    int counter = 0;
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    if(rc != success) {
    	cout << "***** A scan initialization should not fail. *****" << endl;
        cout << "***** [FAIL] Private Extra Credit Test Case 1 Failed. *****" << endl << endl;
		free(tuple);
		free(returnedData);
        return -1;
    } else {
		while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
		{
			counter++;
			if (counter > 2) {
				cout << "***** A wrong entry was returned. RM Private Extra Credit Test Case 1 failed *****" << endl << endl;
				rmsi.close();
				free(tuple);
				free(returnedData);
				return -1;
			}

			int strLength = *(int *)((char *)returnedData + 1);
			std::string strReturned (((char *)returnedData + 5), strLength);
		
			cout << "Returned message_text: " << strReturned << endl;
		}
	} 
	
	rmsi.close();
	
	if (counter != 2) {
    	cout << "***** A scan result is not correct. *****" << endl;
		cout << "***** [FAIL] Private Extra Credit Test Case 1 Failed. *****" << endl << endl;
		free(tuple);
		free(returnedData);
		return -1;
	}   

    free(tuple);
    free(returnedData);

    rc = rm->deleteTable(tableName);

    cout << "***** Private Extra Credit Test Case 1 finished. The result will be examined. *****" << endl;
    return success;
}

int main()
{
    createTweetTable("tbl_private_extra_1");
    RC rcmain = RM_TEST_PRIVATE_EXTRA_1("tbl_private_extra_1");

    return rcmain;
}
