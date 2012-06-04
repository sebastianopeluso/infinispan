package org.infinispan.container;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
public class MVDataContainerTest extends SimpleDataContainerTest {
   @Override
   protected DataContainer createContainer() {
      return new MultiVersionDataContainer(16);
   }
}
