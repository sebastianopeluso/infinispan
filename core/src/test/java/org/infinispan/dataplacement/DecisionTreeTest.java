package org.infinispan.dataplacement;

import org.infinispan.dataplacement.c50.tree.DecisionTreeParser;
import org.infinispan.dataplacement.c50.tree.ParseTreeNode;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Decision Tree test
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.DecisionTreeTest")
public class DecisionTreeTest {

   public void testEx1() throws Exception {
      DecisionTreeParser parser = new DecisionTreeParser("ex1.tree");
      ParseTreeNode root = parser.parse();

      assertNode(root, 3, "2", new int[] {0,0,0,3,2,3,0,0,0,0}, "name", 5, null,
                 new String[][] {new String[] {"N/A"},
                                 new String[] {"peter"},
                                 new String[] {"per"},
                                 new String[] {"por"},
                                 new String[] {"par\\,","pir \\\"na"}});
      ParseTreeNode[] forks = root.getForks();

      assertNode(forks[0], 0, "2", null, null, 0, null, null);
      assertNode(forks[1], 0, "3", new int[] {0,0,0,0,2,0,0,0,0,0}, null, 0, null, null);
      assertNode(forks[2], 0, "2", new int[] {0,0,0,3,0,0,0,0,0,0}, null, 0, null, null);
      assertNode(forks[3], 0, "4", new int[] {0,0,0,0,0,3,0,0,0,0}, null, 0, null, null);
      assertNode(forks[4], 0, "2", null, null, 0, null, null);

   }

   public void testEx2() throws Exception {
      DecisionTreeParser parser = new DecisionTreeParser("ex2.tree");
      ParseTreeNode root = parser.parse();

      assertNode(root, 1, "2", new int[] {0,0,0,3,2,3,0,0,0,0}, "name", 4, null, null);
      ParseTreeNode[] forks = root.getForks();

      assertNode(forks[0], 0, "2", null, null, 0, null, null);
      assertNode(forks[1], 0, "3", new int[] {0,0,0,0,2,0,0,0,0,0}, null, 0, null, null);
      assertNode(forks[2], 0, "2", new int[] {0,0,0,3,0,0,0,0,0,0}, null, 0, null, null);
      assertNode(forks[3], 0, "4", new int[] {0,0,0,0,0,3,0,0,0,0}, null, 0, null, null);
   }

   public void testEx3() throws Exception {
      DecisionTreeParser parser = new DecisionTreeParser("ex3.tree");
      ParseTreeNode root = parser.parse();

      assertNode(root, 2, "5", new int[] {0,0,0,0,1,1,2,2,0,1}, "key_index", 3, "27", null);
      ParseTreeNode[] forks = root.getForks();

      assertNode(forks[0], 0, "5", null, null, 0, null, null);
      assertNode(forks[1], 0, "3", new int[] {0,0,0,0,1,0,0,0,0,1}, null, 0, null, null);
      assertNode(forks[2], 0, "5", new int[] {0,0,0,0,0,1,2,2,0,0}, null, 0, null, null);
   }

   public void testBig() throws Exception {
      DecisionTreeParser parser = new DecisionTreeParser("big");
      ParseTreeNode root = parser.parse();

      //too big to do all the cases. test only the root      
      assertNode(root, 2, "5", new int[] {0,0,39,34,45,5,46,31,25,13}, "thread_index", 3, "8", null);
   }

   private void assertNode(ParseTreeNode node, int type, String clazz, int[] freq, String att, int numberOfForks,
                           String cut, String[][] elts) {
      assert node != null;
      assert node.getType() == type;
      assert node.getClazz().equals(clazz);
      if (freq != null) {
         assert node.getFrequency() != null;
         assert freq.length == node.getFrequency().length;
         for (int i = 0; i < freq.length; ++i) {
            assert freq[i] == node.getFrequency()[i];
         }
      } else {
         assert node.getFrequency() == null;
      }
      if (att != null) {
         assert node.getAttribute() != null;
         assert att.equals(node.getAttribute());
      } else {
         assert node.getAttribute() == null;
      }
      assert node.getNumberOfForks() == numberOfForks;
      if (cut != null) {
         assert node.getCut() != null;
         assert cut.equals(node.getCut());
      } else {
         assert node.getCut() == null;
      }
      if (elts != null) {
         assert elts.length == node.getElts().length;
         for (int i = 0; i < elts.length; ++i) {
            List<String> eltsValues = node.getElts()[i].getValues();
            assert eltsValues.size() == elts[i].length;
            for (int j = 0; j < elts[i].length; ++j) {
               assert elts[i][j].equals(eltsValues.get(j));
            }
         }
      } else {
         assert node.getElts() == null;
      }
   }

}
