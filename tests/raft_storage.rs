//! 用 openraft 自带的协议一致性测试套件验证存储实现。
//!
//! # 这个测试为什么是最重要的
//!
//! 我们在 `docs/DECISIONS.md` 的 ADR-004 里选择 openraft 而非手写 Raft，核心理由
//! 不是「有现成库」，而是**它自带一套协议一致性测试**。
//!
//! Raft 最危险的失败模式不是「跑不起来」，而是「跑得起来但偶发静默丢数据」——
//! 这类问题自己写的单元测试几乎不可能发现，因为测试的作者和实现的作者是同一个人，
//! 会犯同一个错误。
//!
//! `Suite::test_all` 用 openraft 作者对协议的理解来检验我们的存储，覆盖 35 个场景：
//! 日志清除、截断、初始状态推导、成员变更、快照传输、重放已提交日志……
//!
//! **它通过，才谈得上「这个存储实现是对的」。**

use std::sync::{Arc, Mutex};

use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};

use xrl_db::raft::TypeConfig;
use xrl_db::raft::log_store::LogStore;
use xrl_db::raft::state_machine::StateMachine;

/// 存储构建器。
///
/// 套件会反复调用 [`StoreBuilder::build`] 来获得一套全新的、互不干扰的存储，
/// 因为不同用例之间不能有状态残留。
struct Builder {
    /// 留住临时目录。
    ///
    /// `TempDir` 在析构时会删除目录，因此必须把它存下来，否则存储会在测试进行到
    /// 一半时随着目录消失而失效。
    dirs: Mutex<Vec<tempfile::TempDir>>,
}

impl StoreBuilder<TypeConfig, LogStore, StateMachine, ()> for Builder {
    async fn build(&self) -> Result<((), LogStore, StateMachine), StorageError<u64>> {
        // redb 是单进程的：每个存储实例必须有自己的文件，测试也一样。
        // 若共用同一个文件，后创建的那个会直接打不开。
        let dir = tempfile::tempdir().expect("应能创建临时目录");
        let path = dir.path().join("raft.redb");
        let db = Arc::new(redb::Database::create(&path).expect("应能创建数据库"));

        let log_store = LogStore::new(Arc::clone(&db))?;
        let state_machine = StateMachine::new(db)?;

        self.dirs.lock().expect("互斥锁不应中毒").push(dir);

        Ok(((), log_store, state_machine))
    }
}

#[test]
fn storage_satisfies_the_raft_protocol() {
    let builder = Builder {
        dirs: Mutex::new(Vec::new()),
    };

    Suite::test_all(builder).expect(
        "存储实现未通过 openraft 的协议一致性测试。\
         这意味着它**不能**安全地用于复制——宁可现在失败，也不要带着静默丢数据的缺陷上线。",
    );
}
