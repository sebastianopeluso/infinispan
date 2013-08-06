/*
 * INESC-ID, Instituto de Engenharia de Sistemas e Computadores Investigação e Desevolvimento em Lisboa
 * Copyright 2013 INESC-ID and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.tx.gmu;

import org.infinispan.util.concurrent.locks.OwnableReentrantReadWriteLock;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.gmu.OwnableReadWriteLockTest")
public class OwnableReadWriteLockTest {

   private static final Log log = LogFactory.getLog(OwnableReadWriteLockTest.class);
   private static final int NUMBER_OF_THREADS = 64;
   private static final long TIMEOUT = 10000;

   public void testMultipleRead() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);


      for (int i = 0; i < 100; i++) {
         lockShared(lock, owner, true);
      }

      assert lock.getLockState() == -100;

      for (int i = 0; i < 100; i++) {
         lock.unlock(owner);
      }

      assert lock.getLockState() == 0;
   }

   public void testReadWrite() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);

      lockShared(lock, owner, true);

      assert lock.getLockState() == -1;


      boolean result = lock.tryLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assert !result;

      assert lock.getLockState() == -1;

      lock.unlock(owner);

      assert lock.getLockState() == 0;
   }

   public void testWriteRead() throws InterruptedException {
      OwnableReentrantReadWriteLock lock = newLock();
      Owner owner = owner(0);

      lock.tryLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assert lock.getLockState() == 1;
      lockShared(lock, owner, true);
      assert lock.getLockState() == 1;
      lock.unlock(owner);
      assert lock.getLockState() == 0;
   }

   public void testMultipleThreads() {
      final WorkerThread[] workerThreads = new WorkerThread[NUMBER_OF_THREADS];
      final OwnableReentrantReadWriteLock lock1 = new OwnableReentrantReadWriteLock();
      final OwnableReentrantReadWriteLock lock2 = new OwnableReentrantReadWriteLock();
      final SharedObject sharedObject = new SharedObject(lock1, lock2);
      for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
         workerThreads[i] = new WorkerThread(sharedObject, i);
      }
      for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
         workerThreads[i].start();
      }
      try {
         Thread.sleep(60000);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
         workerThreads[i].interrupt();
      }
      for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
         try {
            workerThreads[i].join();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
      assert lock1.getLockState() == 0;
      assert lock2.getLockState() == 0;
      for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
         assert workerThreads[i].errors == 0;
      }
   }

   private Owner owner(int id) {
      return new Owner(id);
   }

   private OwnableReentrantReadWriteLock newLock() {
      return new OwnableReentrantReadWriteLock();
   }

   private void lockShared(OwnableReentrantReadWriteLock lock, Owner owner, boolean result) throws InterruptedException {
      boolean locked = lock.tryShareLock(owner, TIMEOUT, TimeUnit.MILLISECONDS);
      assertEquals(result, locked);
   }

   private static class Owner {
      private final int id;

      private Owner(int id) {
         this.id = id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Owner owner = (Owner) o;

         return id == owner.id;

      }

      @Override
      public int hashCode() {
         return id;
      }
   }

   private class WorkerThread extends Thread {

      private final SharedObject sharedObject;
      private final int id;
      private final Random random;
      private final boolean transferFrom1To2;
      private volatile int errors;
      private volatile boolean running;

      private WorkerThread(SharedObject sharedObject, int id) {
         this.sharedObject = sharedObject;
         this.id = id;
         this.random = new Random(System.nanoTime() << id);
         this.errors = 0;
         this.transferFrom1To2 = id % 2 == 0;
      }

      @Override
      public void run() {
         running = true;
         while (running) {
            executeWrite();
            executeRead();
         }
      }

      @Override
      public void interrupt() {
         running = false;
         super.interrupt();
      }

      private void executeWrite() {
         boolean locked1 = false;
         boolean locked2 = false;
         try {
            locked1 = sharedObject.lock1.tryLock(id, 100, TimeUnit.MILLISECONDS);
            if (!locked1) {
               return;
            }
            locked2 = sharedObject.lock2.tryLock(id, 100, TimeUnit.MILLISECONDS);
            if (!locked2) {
               return;
            }
            if (!sharedObject.lock1.tryShareLock(id, 100, TimeUnit.MILLISECONDS)) {
               log.fatalf("Could not acquire shared lock1 after acquired exclusive lock for thread %s", id);
               errors++;
               return;
            }
            if (!sharedObject.lock2.tryShareLock(id, 100, TimeUnit.MILLISECONDS)) {
               log.fatalf("Could not acquire shared lock2 after acquired exclusive lock for thread %s", id);
               errors++;
               return;
            }

            if (transferFrom1To2) {
               int transfer = random.nextInt(sharedObject.counter1);
               sharedObject.counter1 -= transfer;
               sharedObject.counter2 += transfer;
            } else {
               int transfer = random.nextInt(sharedObject.counter2);
               sharedObject.counter2 -= transfer;
               sharedObject.counter1 += transfer;
            }
         } catch (InterruptedException e) {
            super.interrupt();
         } finally {
            if (locked1) {
               sharedObject.lock1.unlock(id);
            }
            if (locked2) {
               sharedObject.lock2.unlock(id);
            }
         }
      }

      private void executeRead() {
         boolean locked1 = false;
         boolean locked2 = false;
         try {
            locked1 = sharedObject.lock1.tryShareLock(id, 100, TimeUnit.MILLISECONDS);
            if (!locked1) {
               return;
            }
            locked2 = sharedObject.lock2.tryShareLock(id, 100, TimeUnit.MILLISECONDS);
            if (!locked2) {
               return;
            }

            if (!sharedObject.check()) {
               log.fatalf("Inconsistency detected for thread %s", id);
               errors++;
            }
         } catch (InterruptedException e) {
            this.interrupt();
         } finally {
            if (locked1) {
               sharedObject.lock1.unlock(id);
            }
            if (locked2) {
               sharedObject.lock2.unlock(id);
            }
         }
      }
   }

   private class SharedObject {
      private final OwnableReentrantReadWriteLock lock1;
      private final OwnableReentrantReadWriteLock lock2;
      private volatile int counter1 = 10000;
      private volatile int counter2 = 10000;

      private SharedObject(OwnableReentrantReadWriteLock lock1, OwnableReentrantReadWriteLock lock2) {
         this.lock1 = lock1;
         this.lock2 = lock2;
      }

      public final boolean check() {
         return counter2 + counter1 == 20000;
      }
   }

}
