package com.nameof.zookeeper.tools.lock;

/**
 * @Author: chengpan
 * @Date: 2018/11/21
 */
public enum ReadWriteLockState {
    NONE {
        @Override
        public void acceptLockState(ReentrantZkReadWriteLock lockContext) {
            lockContext.setLockState(this);
        }
    },

    READ {
        @Override
        public void acceptLockState(ReentrantZkReadWriteLock lockContext) {
            if (lockContext.getLockState() == WRITE)
                throw new IllegalStateException("unsupport read lock upgrading");
            lockContext.setLockState(this);
        }
    },

    WRITE {
        @Override
        public void acceptLockState(ReentrantZkReadWriteLock lockContext) {
            if (lockContext.getLockState() == READ)
                throw new IllegalStateException("unsupport write lock downgrading");
            lockContext.setLockState(this);
        }
    };

    public abstract void acceptLockState(ReentrantZkReadWriteLock lockContext);
}
