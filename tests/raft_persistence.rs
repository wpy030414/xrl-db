//! 重启恢复：状态机必须能从快照中恢复。
//!
//! # 为什么这个测试必须单独写
//!
//! `Suite::test_all` 覆盖的是存储实现的**协议正确性**，但它每次都用一套全新的存储，
//! 因此**测不到「同一份存储重启后能否恢复」**。
//!
//! 而这恰恰是我们自己设计决策的落点：状态机内容只随快照持久化，`applied_state`
//! 与快照严格同步（详见 `src/raft/state_machine.rs` 的模块说明）。
//!
//! 这条路径一旦出错，症状是**静默丢数据或重复执行非幂等操作**——两者都不会立刻
//! 被发现。因此必须有测试盯住它。

use std::sync::Arc;

use bytes::Bytes;
use openraft::impls::BasicNode;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, StoredMembership};

use xrl_db::kv::WriteOp;
use xrl_db::protocol::SetCondition;
use xrl_db::raft::TypeConfig;
use xrl_db::raft::state_machine::StateMachine;

/// 构造一个日志 ID。
fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 1), index)
}

/// 构造一条「写入」日志条目。
fn write_entry(index: u64, key: &str, value: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(1, index),
        payload: EntryPayload::Normal(WriteOp::Set {
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            expire_at: None,
            condition: SetCondition::Always,
        }),
    }
}

/// 取出快照的原始字节，便于比较。
async fn snapshot_bytes(sm: &mut StateMachine) -> Vec<u8> {
    let mut builder = sm.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.expect("应能生成快照");
    snapshot.snapshot.into_inner()
}

#[tokio::test]
async fn state_machine_recovers_from_snapshot_after_restart() {
    let dir = tempfile::tempdir().expect("应能创建临时目录");
    let path = dir.path().join("state-machine.redb");

    // ---------- 第一次运行：应用条目，生成快照 ----------
    let (applied_before, data_before) = {
        let db = Arc::new(redb::Database::create(&path).expect("应能创建数据库"));
        let mut sm = StateMachine::new(Arc::clone(&db)).expect("应能创建状态机");

        let replies = sm
            .apply(vec![
                write_entry(1, "alpha", "1"),
                write_entry(2, "beta", "2"),
            ])
            .await
            .expect("应能应用日志");
        assert_eq!(replies.len(), 2, "每条日志条目都应对应一个响应");

        // 快照的日志位置必须与状态机报告的已应用位置一致——这是核心不变量
        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("应能生成快照");
        let (applied, _) = sm.applied_state().await.expect("应能读取已应用位置");

        assert_eq!(
            applied, snapshot.meta.last_log_id,
            "快照位置与已应用位置必须一致，否则重启后会丢失或重复应用"
        );

        let data = snapshot.snapshot.into_inner();
        (applied, data)
        // 作用域结束，`db` 被丢弃，redb 释放文件锁
    };

    assert_eq!(applied_before.map(|id| id.index), Some(2));

    // ---------- 第二次运行：重新打开同一文件 ----------
    let db = Arc::new(redb::Database::create(&path).expect("应能重新打开数据库"));
    let mut restored = StateMachine::new(db).expect("应能从快照恢复");

    let (applied_after, _) = restored.applied_state().await.expect("应能读取已应用位置");
    assert_eq!(
        applied_after.map(|id| id.index),
        Some(2),
        "重启后报告的已应用位置必须是快照的位置——报小了会导致重放已应用的条目\
         （`INCR` 这类非幂等操作会被执行两次），报大了会丢失数据"
    );

    // 数据本身也必须恢复了：再导出一份快照，内容应与第一次完全相同
    let data_after = snapshot_bytes(&mut restored).await;
    assert_eq!(
        data_after, data_before,
        "重启后的状态机内容与快照不一致，说明恢复过程丢失或篡改了数据"
    );
}

#[tokio::test]
async fn restored_state_machine_continues_applying_correctly() {
    let dir = tempfile::tempdir().expect("应能创建临时目录");
    let path = dir.path().join("continue.redb");

    // 第一次运行：写一个计数器
    {
        let db = Arc::new(redb::Database::create(&path).expect("应能创建数据库"));
        let mut sm = StateMachine::new(db).expect("应能创建状态机");
        sm.apply(vec![write_entry(1, "counter", "10")])
            .await
            .expect("应能应用");

        let mut builder = sm.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("应能生成快照");
    }

    // 第二次运行：从快照恢复，然后继续在旧值上做自增
    let db = Arc::new(redb::Database::create(&path).expect("应能重新打开数据库"));
    let mut restored = StateMachine::new(db).expect("应能恢复");

    let reply = restored
        .apply(vec![Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(WriteOp::IncrBy {
                key: Bytes::from_static(b"counter"),
                delta: 5,
            }),
        }])
        .await
        .expect("应能应用")
        .into_iter()
        .next()
        .expect("应有响应");

    // 10 + 5 = 15。若恢复失败（计数器被当成不存在），这里会得到 5——
    // 那正是「静默丢数据」在数字上的样子。
    assert_eq!(
        reply,
        xrl_db::protocol::Reply::Integer(15),
        "恢复后自增应基于快照中的旧值，而不是从 0 重新开始"
    );
}

#[tokio::test]
async fn fresh_database_starts_empty() {
    // 对照组：全新的数据库不应凭空出现数据
    let dir = tempfile::tempdir().expect("应能创建临时目录");
    let path = dir.path().join("fresh.redb");

    let db = Arc::new(redb::Database::create(&path).expect("应能创建数据库"));
    let mut sm = StateMachine::new(db).expect("应能创建状态机");

    let (applied, membership) = sm.applied_state().await.expect("应能读取状态");
    assert_eq!(applied, None, "全新数据库不应有任何已应用记录");
    assert_eq!(
        membership,
        StoredMembership::<u64, BasicNode>::default(),
        "全新数据库不应有成员配置"
    );
    assert!(sm.is_empty(), "全新数据库的状态机应为空");
}
