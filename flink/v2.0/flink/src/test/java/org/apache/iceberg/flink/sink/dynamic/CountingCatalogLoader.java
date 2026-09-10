/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.dynamic;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A {@link CatalogLoader} which counts the catalogs it hands out and the ones which are closed, so
 * a test can assert that an operator released every catalog it opened. A created-catalog count on
 * its own cannot tell a leak from a close.
 *
 * <p>The counters are static and keyed by an id the loader carries, because Flink serializes the
 * loader to the tasks: within a MiniCluster those tasks run in this JVM, so they share the
 * counters, while each test keeps its own pair through its own id.
 */
class CountingCatalogLoader implements CatalogLoader {

  private static final Map<String, AtomicInteger> OPENED = Maps.newConcurrentMap();
  private static final Map<String, AtomicInteger> CLOSED = Maps.newConcurrentMap();

  private final String id;
  private final CatalogLoader delegate;

  CountingCatalogLoader(String id, CatalogLoader delegate) {
    this.id = id;
    this.delegate = delegate;
  }

  /** Number of catalogs handed out under {@code id}. */
  static int opened(String id) {
    return counter(OPENED, id).get();
  }

  /** Number of catalogs closed under {@code id}. */
  static int closed(String id) {
    return counter(CLOSED, id).get();
  }

  /** Catalogs opened but never closed under {@code id}. Zero is the only healthy value. */
  static int leaked(String id) {
    return opened(id) - closed(id);
  }

  static void reset(String id) {
    counter(OPENED, id).set(0);
    counter(CLOSED, id).set(0);
  }

  @Override
  public Catalog loadCatalog() {
    Catalog catalog = delegate.loadCatalog();
    counter(OPENED, id).incrementAndGet();
    return counting(id, catalog);
  }

  @Override
  @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
  public CatalogLoader clone() {
    return new CountingCatalogLoader(id, delegate.clone());
  }

  @Override
  public String toString() {
    return "CountingCatalogLoader(" + id + ", " + delegate + ")";
  }

  private static AtomicInteger counter(Map<String, AtomicInteger> counters, String id) {
    return counters.computeIfAbsent(id, unused -> new AtomicInteger());
  }

  /**
   * Wraps the catalog so that {@code close()} is counted before it reaches the delegate. The
   * wrapper is a proxy rather than a hand-written delegate because the sink reaches for {@link
   * SupportsNamespaces} and {@link Closeable} on the same object.
   */
  private static Catalog counting(String id, Catalog delegate) {
    return (Catalog)
        Proxy.newProxyInstance(
            CountingCatalogLoader.class.getClassLoader(),
            new Class<?>[] {Catalog.class, SupportsNamespaces.class, Closeable.class},
            (proxy, method, args) -> {
              if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                counter(CLOSED, id).incrementAndGet();
                if (delegate instanceof Closeable) {
                  ((Closeable) delegate).close();
                }
                return null;
              }

              try {
                Object result = method.invoke(delegate, args);
                return result == delegate ? proxy : result;
              } catch (InvocationTargetException e) {
                throw e.getCause();
              }
            });
  }
}
