package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;

import java.sql.SQLException;
import java.util.BitSet;

public class ValuesNativeInputFormat implements NativeInputFormat {

    private final SQLLexer lexer;

    public ValuesNativeInputFormat(int pos, String sql) {
        this.lexer = new SQLLexer(pos, sql);
    }

    @Override
    public void fill(Block block) throws SQLException {
        BitSet constIdxFlags = new BitSet(block.columnCnt());
        for (; ; ) {
            char nextChar = lexer.character();
            if (lexer.eof() || nextChar == ';') {
                break;
            }

            if (nextChar == ',') {
                nextChar = lexer.character();
            }
            Validate.isTrue(nextChar == '(');
            for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
                if (columnIdx > 0) {
                    Validate.isTrue(lexer.character() == ',');
                }
                constIdxFlags.set(columnIdx);
                block.setObject(columnIdx, block.getColumn(columnIdx).type().deserializeText(lexer));
            }
            Validate.isTrue(lexer.character() == ')');
            block.appendRow();
        }

        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (constIdxFlags.get(columnIdx)) {
                block.incPlaceholderIndexes(columnIdx);
            }
        }
    }
}
