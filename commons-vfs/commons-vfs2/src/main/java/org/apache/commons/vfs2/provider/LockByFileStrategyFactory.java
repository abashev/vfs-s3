package org.apache.commons.vfs2.provider;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock operations with file only on file instance - useful for stateless file systems
 *
 * @author <A href="mailto:alexey at abashev dot ru">Alexey Abashev</A>
 */
public class LockByFileStrategyFactory implements FileLockStrategyFactory {
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public Lock createLock() {
        return lock;
    }
}
