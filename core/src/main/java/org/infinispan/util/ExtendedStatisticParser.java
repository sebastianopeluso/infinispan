package org.infinispan.util;

import org.infinispan.distribution.wrappers.CustomStatsInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ExtendedStatisticParser {
   private static final String TRANSACTION_CLASS_SUFFIX = "ForTxClass";
   private static final String DISPLAY_TRANSACTION_CLASS_SUFFIX = " per tx class";
   private static final int MAX_RHQ_SIZE = 100;
   private static final String PROCESS_ENABLE = "processEnable";
   private static final String PROCESS_COMMON = "processCommon";
   private static final String GENERATE_CODE = "generateCode";
   private static final String[] IGNORE_ENABLE = new String[]{
         "gMUWaitingTimeEnabled",
         "enabled",
         "resetStatistics"
   };
   private static final String[] IGNORE_COMMON = new String[]{
         "numNodes",
         "replicationDegree",
         "localActiveTransactions",
   };
   private static final String NEW_LINE = System.getProperty("line.separator");

   public static void main(String[] args) {
      MBean mBean = findAnnotation(CustomStatsInterceptor.class, MBean.class);
      if (mBean == null) {
         return;
      }
      Filter filter = new Filter(!Boolean.getBoolean(PROCESS_ENABLE), !Boolean.getBoolean(PROCESS_COMMON));
      Map<String, Statistic> statisticMap = new HashMap<String, Statistic>();

      process(CustomStatsInterceptor.class, statisticMap, filter);

      Analyzer analyzer = new ValidateStatisticsAnalyzer();
      analyzer.analyze(statisticMap, mBean);
      System.out.println("Analyzed " + statisticMap.size() + " statistics");
      System.exit(0);
   }

   private static <T extends Annotation> T findAnnotation(Class clazz, Class<T> annotationClass) {
      if (clazz == null) {
         return null;
      }
      T annotation = (T) clazz.getAnnotation(annotationClass);
      if (annotation != null) {
         return annotation;
      }
      if (clazz == Object.class) {
         return null;
      }
      annotation = findAnnotation(clazz.getSuperclass(), annotationClass);
      if (annotation == null) {
         for (Class interfaceClass : clazz.getInterfaces()) {
            annotation = findAnnotation(interfaceClass, annotationClass);
            if (annotation != null) {
               return annotation;
            }
         }
      }
      return annotation;
   }

   private static void process(Class clazz, Map<String, Statistic> statisticMap, Filter filter) {
      if (clazz == null) {
         return;
      }
      for (Method method : clazz.getDeclaredMethods()) {
         if (method.getAnnotation(ManagedAttribute.class) != null) {
            processAttribute(method, statisticMap, filter);
         } else if (method.getAnnotation(ManagedOperation.class) != null) {
            processOperation(method, statisticMap, filter);
         }
      }
      if (clazz == Object.class) {
         return;
      }
      process(clazz.getSuperclass(), statisticMap, filter);
      for (Class interfaceClass : clazz.getInterfaces()) {
         process(interfaceClass, statisticMap, filter);
      }
   }

   private static void processOperation(Method method, Map<String, Statistic> statisticMap, Filter filter) {
      ManagedOperation managedOperation = method.getAnnotation(ManagedOperation.class);
      String attributeName;
      boolean txClassStats = false;

      if (method.getName().endsWith(TRANSACTION_CLASS_SUFFIX)) {
         attributeName = extractNameFromTxClass(method.getName());
         txClassStats = true;
      } else {
         attributeName = extractNameFromAttribute(method.getName());
      }
      if (filter.skip(attributeName)) {
         return;
      }

      String description = managedOperation.description();
      String displayName = managedOperation.displayName();
      String type = method.getReturnType().getName();

      Statistic statistic = statisticMap.get(attributeName);
      if (statistic == null) {
         statistic = new Statistic();
         statisticMap.put(attributeName, statistic);
         statistic.attributeName = attributeName;
         statistic.description = description;
         statistic.displayName = displayName;
         statistic.type = type;
      } else {
         if (statistic.description == null || statistic.description.isEmpty()) {
            statistic.description = description;
         }
         if (statistic.displayName == null || statistic.displayName.isEmpty()) {
            statistic.displayName = displayName;
         }
         if (!statistic.type.equals(type)) {
            System.err.println("Mismatch type: " + type + " vs " + statistic.type);
         }
      }
      statistic.collectedInTxClass = statistic.collectedInTxClass || txClassStats;
      statistic.collected = statistic.collected || !txClassStats;

   }

   private static void processAttribute(Method method, Map<String, Statistic> statisticMap, Filter filter) {
      ManagedAttribute managedAttribute = method.getAnnotation(ManagedAttribute.class);
      String attributeName = extractNameFromAttribute(method.getName());
      if (filter.skip(attributeName)) {
         return;
      }

      String description = managedAttribute.description();
      String displayName = managedAttribute.displayName();
      String type = method.getReturnType().getName();
      Statistic statistic = statisticMap.get(attributeName);
      if (statistic == null) {
         statistic = new Statistic();
         statisticMap.put(attributeName, statistic);
         statistic.attributeName = attributeName;
         statistic.description = description;
         statistic.displayName = displayName;
         statistic.type = type;
      } else {
         if (statistic.description == null || statistic.description.isEmpty()) {
            statistic.description = description;
         }
         if (statistic.displayName == null || statistic.displayName.isEmpty()) {
            statistic.displayName = displayName;
         }
         if (!statistic.type.equals(type)) {
            System.err.println("Mismatch type: " + type + " vs " + statistic.type);
         }
      }
      statistic.collected = true;
   }

   private static String extractNameFromAttribute(String methodName) {
      String name;
      if (methodName.startsWith("set") || methodName.startsWith("get")) {
         name = methodName.substring(3);
      } else if (methodName.startsWith("is")) {
         name = methodName.substring(2);
      } else {
         name = methodName;
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Character.toLowerCase(name.charAt(0)));
      if (name.length() > 2) sb.append(name.substring(1));
      return sb.toString();
   }

   private static String extractNameFromTxClass(String methodName) {
      String name;
      int endIndex = methodName.indexOf("ForTxClass");
      if (methodName.startsWith("set") || methodName.startsWith("get")) {
         name = methodName.substring(3, endIndex);
      } else if (methodName.startsWith("is")) {
         name = methodName.substring(2, endIndex);
      } else {
         name = methodName.substring(0, endIndex);
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Character.toLowerCase(name.charAt(0)));
      if (name.length() > 2) sb.append(name.substring(1));
      return sb.toString();
   }

   @SuppressWarnings("StringBufferReplaceableByString")
   private static String capitalize(String string) {
      if (string == null || string.isEmpty()) {
         return string;
      }
      StringBuilder sb = new StringBuilder(string.length())
            .append(Character.toUpperCase(string.charAt(0)));
      if (string.length() > 2) {
         sb.append(string.substring(1));
      }
      return sb.toString();
   }

   private static interface Analyzer {
      void analyze(Map<String, Statistic> statisticMap, MBean mBean);
   }

   private static class Statistic {
      private String attributeName;
      private String description;
      private String type;
      private String displayName;
      private boolean collected;
      private boolean collectedInTxClass;
   }

   private static class ValidateStatisticsAnalyzer implements Analyzer {

      @Override
      public void analyze(Map<String, Statistic> statisticMap, MBean mBean) {
         final int fixedLength = mBean.objectName().length() + DISPLAY_TRANSACTION_CLASS_SUFFIX.length();
         final boolean generateCode = Boolean.getBoolean(GENERATE_CODE);
         List<String> errors = new ArrayList<String>();
         StringBuilder code = new StringBuilder();
         for (Statistic statistic : statisticMap.values()) {
            if (statistic.collected && !statistic.collectedInTxClass) {
               errors.add("[" + statistic.attributeName + "] missing tx class get method!");
               if (generateCode) {
                  code.append("@ManagedOperation(description=\"").append(statistic.description)
                        .append(DISPLAY_TRANSACTION_CLASS_SUFFIX).append("\",").append(NEW_LINE);
                  code.append("                  displayName=\"").append(statistic.displayName)
                        .append(DISPLAY_TRANSACTION_CLASS_SUFFIX).append("\")").append(NEW_LINE);
                  code.append("public final ").append(statistic.type)
                        .append(" get").append(capitalize(statistic.attributeName)).append(TRANSACTION_CLASS_SUFFIX)
                        .append("(@Parameter(name = \"Transaction Class\") String txClass) {").append(NEW_LINE);
                  if (statistic.type.contains("long")) {
                     code.append("\treturn handleLong((Long) TransactionsStatisticsRegistry.getAttribute(XXX, txClass));")
                           .append(NEW_LINE);
                  } else {
                     code.append("\treturn handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(XXX, txClass));")
                           .append(NEW_LINE);
                  }
                  code.append("}").append(NEW_LINE);
                  code.append(NEW_LINE);
               }
            }
            if (!statistic.collected && statistic.collectedInTxClass) {
               errors.add("[" + statistic.attributeName + "] missing default get method!");
               if (generateCode) {
                  code.append("@ManagedAttribute(description=\"").append(statistic.description).append("\",").append(NEW_LINE);
                  code.append(                  "displayName=\"").append(statistic.displayName).append("\")").append(NEW_LINE);
                  code.append("public final ").append(statistic.type).append(" get")
                        .append(capitalize(statistic.attributeName)).append("() {").append(NEW_LINE);
                  if (statistic.type.contains("long")) {
                     code.append("\treturn handleLong((Long) TransactionsStatisticsRegistry.getAttribute(XXX, null));")
                           .append(NEW_LINE);
                  } else {
                     code.append("\treturn handleDouble((Double) TransactionsStatisticsRegistry.getAttribute(XXX, null));")
                           .append(NEW_LINE);
                  }
                  code.append("}").append(NEW_LINE);
                  code.append(NEW_LINE);
               }
            }
            final int length = statistic.displayName.length() + fixedLength;
            if (length >= MAX_RHQ_SIZE) {
               errors.add("[" + statistic.attributeName + "] possible too long name (" + length + ")");
            }
         }

         if (!errors.isEmpty()) {
            System.out.println("####################### ERRORS #####################");
            for (String error : errors) {
               System.out.println(error);
            }
         }

         if (generateCode && code.length() != 0) {
            System.out.println("################### GENERATED CODE #################");
            System.out.println(code);
         }

      }
   }

   private static class Filter {

      private final Set<String> ignore;

      private Filter(boolean skipEnable, boolean skipCommon) {
         if (!skipCommon && !skipEnable) {
            ignore = Collections.emptySet();
         } else if (skipCommon && !skipEnable) {
            ignore = new HashSet<String>(Arrays.asList(IGNORE_COMMON));
         } else if (!skipCommon) {
            ignore = new HashSet<String>(Arrays.asList(IGNORE_ENABLE));
         } else {
            ignore = new HashSet<String>(Arrays.asList(IGNORE_COMMON));
            ignore.addAll(Arrays.asList(IGNORE_ENABLE));
         }
      }

      public final boolean skip(String attributeName) {
         return ignore.contains(attributeName);
      }
   }

}
