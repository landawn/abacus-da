/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.da;

import com.landawn.abacus.parser.JsonParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.parser.XmlParser;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.Hex;
import com.landawn.abacus.util.Strings;

public class AbstractNoSQLTest {
    static final JsonParser jsonParser = ParserFactory.createJsonParser();
    static final XmlParser xmlParser = ParserFactory.createXmlParser();

    protected Account createAccount() {
        Account account = Beans.newRandom(Account.class);
        account.setId(generateId());
        return account;
    }

    protected String generateId() {
        return Hex.encodeToString(Strings.uuid().getBytes()).substring(0, 24);
    }
}
