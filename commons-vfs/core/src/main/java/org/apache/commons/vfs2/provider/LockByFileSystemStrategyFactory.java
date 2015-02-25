package org.apache.commons.vfs2.provider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock operations by file system - useful for cacheable file systems with limit access.
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class LockByFileSystemStrategyFactory<AFS extends AbstractFileSystem> implements FileLockStrategyFactory {
    private static final ConcurrentMap<FileSystemKey, Lock> locksByKey = new ConcurrentHashMap<FileSystemKey, Lock>();
    private static final ConcurrentMap<AbstractFileSystem, Lock> locksByObject = new ConcurrentHashMap<AbstractFileSystem, Lock>();

    private final Lock lock;

    public LockByFileSystemStrategyFactory(AFS fileSystem) {
        final FileSystemKey cacheKey = fileSystem.getCacheKey();

        Lock newLock = new ReentrantLock(), oldLock;

        if (cacheKey != null) {
            if ((oldLock = locksByKey.putIfAbsent(cacheKey, newLock)) != null) {
                this.lock = oldLock;
            } else {
                this.lock = newLock;
            }
        } else {
            if ((oldLock = locksByObject.putIfAbsent(fileSystem, newLock)) != null) {
                this.lock = oldLock;
            } else {
                this.lock = newLock;
            }
        }
    }

    @Override
    public Lock createLock() {
        return lock;
    }
}
