use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use common::error::DbError;
use engine::DB;
use tempfile::tempdir;

#[tokio::test]
async fn sequential_transactions_on_same_key_do_not_conflict() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).await.unwrap();

    let mut t1 = db.transaction().await;
    t1.set("k", "v1");
    t1.commit().await.unwrap();

    // t2 begins strictly after t1 committed: there is nothing to conflict with.
    let mut t2 = db.transaction().await;
    t2.set("k", "v2");
    t2.commit()
        .await
        .expect("t2 started after t1 committed and must not conflict");

    assert_eq!(db.get("k").await.unwrap(), Some("v2".to_string()));
}

#[tokio::test]
async fn pruning_recent_on_oldest_commit_does_not_hide_conflicts() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).await.unwrap();

    let mut t1 = db.transaction().await;
    let mut t2 = db.transaction().await;
    let mut t3 = db.transaction().await;

    // t2 commits "k" while t3 is running.
    t2.set("k", "from t2");
    t2.commit().await.unwrap();

    // t1 is the oldest; its commit prunes t2's entry for "k".
    t1.set("other", "x");
    t1.commit().await.unwrap();

    // t3 began before t2 committed "k": first-committer-wins requires abort.
    t3.set("k", "from t3");
    assert!(
        matches!(t3.commit().await, Err(DbError::CommitConflict)),
        "t3 overwrote a key committed after it began (lost update)"
    );
}

#[tokio::test]
async fn snapshot_does_not_see_writes_committed_after_it_began() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).await.unwrap();

    let mut writer = db.transaction().await;
    writer.set("k", "v");

    let reader = db.transaction().await;
    assert_eq!(reader.get("k").await.unwrap(), None);

    writer.commit().await.unwrap();

    assert_eq!(
        reader.get("k").await.unwrap(),
        None,
        "read inside one transaction changed after a concurrent commit"
    );
}

#[tokio::test]
async fn plain_write_to_a_tx_key_is_detected_as_conflict() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).await.unwrap();

    let mut tx = db.transaction().await;
    tx.set("k", "tx");
    db.set("k", "plain").await.unwrap();

    assert!(
        matches!(tx.commit().await, Err(DbError::CommitConflict)),
        "commit succeeded over a concurrent write to the same key"
    );
}

#[tokio::test]
async fn successful_commit_is_visible() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path()).await.unwrap();

    let mut tx = db.transaction().await;
    tx.set("k", "tx");
    db.set("k", "plain").await.unwrap();

    match tx.commit().await {
        Ok(()) => assert_eq!(
            db.get("k").await.unwrap(),
            Some("tx".to_string()),
            "commit returned Ok but the committed value is shadowed"
        ),
        Err(DbError::CommitConflict) => {}
        Err(e) => panic!("unexpected error: {e}"),
    }
}

#[tokio::test]
async fn committed_tx_and_later_writes_survive_crash_recovery() {
    let dir = tempdir().unwrap();
    {
        let db = DB::open(dir.path()).await.unwrap();
        db.set("before", "1").await.unwrap();

        let mut tx = db.transaction().await;
        tx.set("in_tx", "2");
        tx.commit().await.unwrap();

        db.set("after", "3").await.unwrap();
        // Simulated crash: no close(), nothing flushed to SSTables.
        drop(db);
    }

    let db = DB::open(dir.path()).await.unwrap();
    assert_eq!(db.get("before").await.unwrap(), Some("1".to_string()));
    assert_eq!(
        db.get("in_tx").await.unwrap(),
        Some("2".to_string()),
        "committed transaction lost after WAL replay"
    );
    assert_eq!(
        db.get("after").await.unwrap(),
        Some("3".to_string()),
        "plain write after a transaction lost after WAL replay"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_is_atomic_for_concurrent_readers() {
    const KEYS: usize = 50;

    let dir = tempdir().unwrap();
    let db = Arc::new(DB::open(dir.path()).await.unwrap());

    let mut tx = db.transaction().await;
    for i in 0..KEYS {
        tx.set(&format!("k{i:03}"), "v");
    }

    let done = Arc::new(AtomicBool::new(false));
    let partial = Arc::new(AtomicUsize::new(0));
    let reader = tokio::spawn({
        let (db, done, partial) = (db.clone(), done.clone(), partial.clone());
        async move {
            while !done.load(SeqCst) {
                // Separate `db.get` calls each read at the latest watermark, so a
                // commit landing mid-scan is expected; read through one snapshot.
                let snap = db.transaction().await;
                let mut seen = 0;
                for i in 0..KEYS {
                    if snap.get(&format!("k{i:03}")).await.unwrap().is_some() {
                        seen += 1;
                    }
                }
                if seen != 0 && seen != KEYS {
                    partial.store(seen, SeqCst);
                    return;
                }
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }
    });

    tx.commit().await.unwrap();
    done.store(true, SeqCst);
    reader.await.unwrap();

    assert_eq!(
        partial.load(SeqCst),
        0,
        "reader observed a partially applied transaction"
    );
}
