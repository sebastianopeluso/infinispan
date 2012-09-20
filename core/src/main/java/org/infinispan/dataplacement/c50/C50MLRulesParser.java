package org.infinispan.dataplacement.c50;

import org.infinispan.dataplacement.c50.lookup.C50MLTree;
import org.infinispan.dataplacement.c50.lookup.C50MLTreeElement;
import org.infinispan.dataplacement.c50.keyfeature.AbstractFeature;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Machine Learner (C5.0) rules parser.
 *
 * It converts the rules to a tree of features and features values to query
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class C50MLRulesParser {

   private static final int END_OF_RULES = -1;

   private final String[] rules;
   private final Map<String, AbstractFeature> featureMap;

   private int currentRule = 0;
   private int currentLevel = 0;
   private boolean parsed = false;
   private C50MLTree c50MLTree;

   private static final Log log = LogFactory.getLog(C50MLRulesParser.class);

   public C50MLRulesParser(Collection<String> rulesLine, Map<String, AbstractFeature> featureMap) {
      rules = rulesLine.toArray(new String[rulesLine.size()]);
      this.featureMap = featureMap;
      if (rules.length == 0) {
         currentRule = END_OF_RULES;
      }
   }

   /**
    * it parses the machine learner rules and creates a tree with them
    *
    * @return  the tree with the rules
    */
   public synchronized final C50MLTree parseMachineLearnerRules() {
      if (parsed) {
         return c50MLTree;
      }

      C50MLTreeElement rootElement = new C50MLTreeElement();

      if (isASingleRule()) {
         StringTokenizer ruleTokenizer = parseCurrentRule();
         if (ruleTokenizer.countTokens() == 2) {
            rootElement.setLeaf(parseNewOwnerIndex(ruleTokenizer));
         } else {
            parseMachineLeanerRuleLevel(0, rootElement);
         }
      } else {
         parseMachineLeanerRuleLevel(0, rootElement);
      }
      parsed = true;
      c50MLTree = new C50MLTree(rootElement);
      return c50MLTree;
   }

   /**
    * returns true if the machine leaner only has one rule
    *
    * @return  true if the machine leaner only has one rule, false otherwise
    */
   private boolean isASingleRule() {
      return rules.length == 1;
   }

   /**
    * creates a String Tokenizer where each token is part of the rule
    *
    * @return  String Tokenizer where each token is part of the rule
    */
   private StringTokenizer parseCurrentRule() {
      return new StringTokenizer(getCurrentRule(), " :.");
   }

   /**
    * returns the current rule to be parsed
    *
    * @return  the current rule to be parsed
    */
   private String getCurrentRule() {
      return rules[currentRule];
   }

   /**
    * returns the current rule level
    *
    * @return  the current rule level
    */
   private int getCurrentLevel() {
      return currentLevel;
   }

   /**
    * marks the current rule as processed and advances to the next one (if any)
    */
   private void markRuleAsProcessed() {
      currentRule++;
      if (currentRule >= rules.length) {
         currentRule = END_OF_RULES;
         currentLevel = END_OF_RULES;
         return;
      }
      currentLevel = determineLevel();
   }

   /**
    * returns true if it has more rules to be processed
    *
    * @return  true if it has more rules to be processed, false otherwise
    */
   private boolean hasMoreRules() {
      return currentRule != END_OF_RULES;
   }

   /**
    * calculates the rule level for the current rule 
    *
    * @return  the rule level
    */
   private int determineLevel() {
      int counter = 0;
      for (int it = 0; it < getCurrentRule().length(); it++) {
         char c = getCurrentRule().charAt(it);
         if (c == ' ' || c == ':' || c == '.') {
            counter++;
         } else {
            break;
         }
      }
      assert (counter % 4 == 0);

      return counter / 4;
   }

   /**
    * parse the owner node index
    *
    * @param tokenizer  the rule tokenizer
    * @return           the owner node index  
    */
   private int parseNewOwnerIndex(StringTokenizer tokenizer) {
      if (tokenizer.hasMoreElements()) {
         try {
            return Integer.parseInt(tokenizer.nextToken());
         } catch (NumberFormatException nfe) {
            //no-op
         }
      }
      return C50MLTreeElement.NO_LEAF_ELEMENT;
   }

   /**
    * parses the rule and creates a tree element corresponding to the rule
    *
    * @return  the tree element
    */
   private C50MLTreeElement parseTreeElement() {
      StringTokenizer ruleTokenizer = parseCurrentRule();
      String featureName = ruleTokenizer.nextToken();
      AbstractFeature feature = featureMap.get(featureName);
      String condition = ruleTokenizer.nextToken();
      String value = ruleTokenizer.nextToken();
      int newOwnerIndex = parseNewOwnerIndex(ruleTokenizer);

      if (feature == null) {
         log.fatalf("Parsed a feature not identified. Feature is %s and known features are %s", featureName, featureMap.keySet());
         throw new IllegalStateException("Parsed a non-existing Feature. This should not happen");
      }

      return new C50MLTreeElement(newOwnerIndex, feature, value, condition);
   }

   /**
    * parses a set of rules in the same level and put them in the tree
    *
    * @param level   the level
    * @param parent  the parent level
    */
   private void parseMachineLeanerRuleLevel(int level, C50MLTreeElement parent) {
      C50MLTreeElement lastElement = null;
      while (hasMoreRules()) {
         int currentLevel = getCurrentLevel();
         if (currentLevel > level) {
            parseMachineLeanerRuleLevel(currentLevel, lastElement);
         } else if (currentLevel < level) {
            return;
         } else {
            lastElement = parseTreeElement();
            parent.addChild(lastElement);
            markRuleAsProcessed();
         }
      }
   }

}
