package org.infinispan.dataplacement.c50.tree;

import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Decision Tree parser for the output file *.tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTreeParser {

   private static final Log log = LogFactory.getLog(DecisionTreeParser.class);

   private static final String FILE_EXTENSION = ".tree";

   private final String filePath;

   public DecisionTreeParser(String filePath) {
      if (!filePath.endsWith(FILE_EXTENSION)) {
         filePath += FILE_EXTENSION;
      }
      this.filePath = filePath;
   }

   /**
    * parses the tree represented in this instance
    *
    * @return           the root of the decision tree
    * @throws Exception if some errors occurs during the parser
    */
   public final ParseTreeNode parse() throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("Starting to parse file %s", filePath);
      }

      InputStream inputStream = FileLookupFactory.newInstance().lookupFile(filePath,
                                                                           DecisionTreeParser.class.getClassLoader());

      if (inputStream == null) {
         throw new IllegalArgumentException("File '" + filePath + "' not found");
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

      ParseTreeNode root = new ParseTreeNode();
      root.parse(reader);

      if (log.isTraceEnabled()) {
         StringBuilder stringBuilder = new StringBuilder("Tree parsed:\n");
         root.toString(0, stringBuilder);
         log.trace(stringBuilder);
      }

      safeClose(reader);

      return root;
   }

   private void safeClose(Closeable closeable) {
      try {
         closeable.close();
      } catch (IOException e) {
         //just ignore
      }
   }
}
