/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.parsing;

/**
 * @author Clinton Begin
 */
public class GenericTokenParser {

  private final String openToken;
  private final String closeToken;
  private final TokenHandler handler;

  public GenericTokenParser(String openToken, String closeToken, TokenHandler handler) {
    this.openToken = openToken;
    this.closeToken = closeToken;
    this.handler = handler;
  }

  public String parse(String text) {
    if (text == null || text.isEmpty()) {
      return "";
    }
    // search open token
    // 占位符属性一般是 <elem>${xxx}</elem> 或 <elem attr1="${xxx}"/>
    // 但值实际上可以这样："I am MRAG from ${userCity:重庆}"
    int start = text.indexOf(openToken);
    if (start == -1) {
      return text;
    }
    char[] src = text.toCharArray();

    // 用来存每次循环处理的起点下标
    int offset = 0;

    // 用来存储读过的内容
    final StringBuilder builder = new StringBuilder();
    StringBuilder expression = null;

    do {
      if (start > 0 && src[start - 1] == '\\') {
        // this open token is escaped. remove the backslash and continue.
        // '\{openToken}' 会被认为是转义，跳过 '\' 并将 '{openToken}' 看做普通字面量
        // 将 [offset, start-1) 区间的内容添加进 builder，即刚好读到 '\' 之前；然后直接拼接 {openToken}
        builder.append(src, offset, start - offset - 1).append(openToken);
        offset = start + openToken.length(); // offset 移到 \{openToken} 之后作为下一次操作起点
      } else {
        // found open token. let's search close token.
        if (expression == null) {
          expression = new StringBuilder();
        } else {
          expression.setLength(0);
        }

        builder.append(src, offset, start - offset); // 将 [offset, start) 区间的内容添加进 builder
        offset = start + openToken.length(); // 然后移动 offset

        int end = text.indexOf(closeToken, offset); // 然后开始找接着 offset 之后的 closeToken
        while (end > -1) {
          if ((end <= offset) || (src[end - 1] != '\\')) {
            // closeToken 没有被转义的条件是前面没有反斜杠，或者 closeToken 跟 openToken 连着一起的，即中间没有参数
            // 中间没有参数的情况，取决于 TokenHandler 如何处理；mybatis 并没有处理这种情况
            // 理论上如果 properties 含有一个空字符串 key，只要有对应的键值，就可以处理。。。
            expression.append(src, offset, end - offset); // 将中间的表达式添加进 expression
            break;
          }
          // this close token is escaped. remove the backslash and continue.
          expression.append(src, offset, end - offset - 1).append(closeToken);
          offset = end + closeToken.length();
          end = text.indexOf(closeToken, offset);
        }

        if (end == -1) {
          // close token was not found.
          // 找不到配套的 closeToken 就不做处理了，直接当字面量处理
          builder.append(src, start, src.length - start);
          offset = src.length;
        } else {
          // 使用 tokenHandler 进行转义处理，然后添加进 builder
          builder.append(handler.handleToken(expression.toString()));
          offset = end + closeToken.length(); // offset 移位
        }
      }
      // 寻找下一个 openToken；找不到就退出循环
      start = text.indexOf(openToken, offset);
    } while (start > -1);
    if (offset < src.length) {
      // 表达式后面还有字面量则将字面量添加进 builder
      builder.append(src, offset, src.length - offset);
    }

    // 处理结束
    return builder.toString();
  }
}
