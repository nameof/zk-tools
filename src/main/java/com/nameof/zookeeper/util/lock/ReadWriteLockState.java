package com.nameof.zookeeper.util.lock;

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
            if (lockContext.getLockState() != this && lockContext.getLockState() != NONE)
                throw new IllegalStateException("unsupport read lock upgrading");
            lockContext.setLockState(this);
        }
    },

    WRITE {
        @Override
        public void acceptLockState(ReentrantZkReadWriteLock lockContext) {
            if (lockContext.getLockState() != this && lockContext.getLockState() != NONE)
                throw new IllegalStateException("unsupport write lock downgrading");
            lockContext.setLockState(this);
        }
    };

    public abstract void acceptLockState(ReentrantZkReadWriteLock lockContext);
}
