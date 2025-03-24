package com.berry.clickhouse.tcp.client.stream;

import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.misc.SQLLexer;
import com.berry.clickhouse.tcp.client.misc.Validate;

import java.sql.SQLException;
import java.util.BitSet;

/**
 * ValuesWithParametersNativeInputFormat类实现了NativeInputFormat接口
 * 用于填充包含参数的原生输入格式
 */
public class ValuesWithParametersNativeInputFormat implements NativeInputFormat {

    private final SQLLexer lexer; // SQL词法分析器

    /**
     * 构造函数，初始化ValuesWithParametersNativeInputFormat
     * 
     * @param pos 当前位置
     * @param sql SQL查询
     */
    public ValuesWithParametersNativeInputFormat(int pos, String sql) {
        this.lexer = new SQLLexer(pos, sql); // 初始化词法分析器
    }

    @Override
    public void fill(Block block) throws SQLException {
        BitSet constIdxFlags = new BitSet(block.columnCnt()); // 列索引标志
        char nextChar = lexer.character(); // 获取下一个字符
        Validate.isTrue(nextChar == '('); // 验证开始括号
        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (columnIdx > 0) {
                Validate.isTrue(lexer.character() == ','); // 验证逗号分隔
            }

            if (lexer.isCharacter('?')) {
                lexer.character(); // 跳过占位符
            } else {
                constIdxFlags.set(columnIdx); // 设置列索引标志
                block.setObject(columnIdx, block.getColumn(columnIdx).type()); // 反序列化并设置对象
            }
        }

        for (int columnIdx = 0; columnIdx < block.columnCnt(); columnIdx++) {
            if (constIdxFlags.get(columnIdx)) {
                block.incPlaceholderIndexes(columnIdx); // 增加占位符索引
            }
        }
        Validate.isTrue(lexer.character() == ')'); // 验证结束括号
    }
}
