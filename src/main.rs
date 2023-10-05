mod db;
mod listener;
mod protocol;
mod task_manager;
mod types;

use rand::seq::SliceRandom;
use rand::thread_rng;

use types::*;

#[derive(Debug)]
pub enum ChannelMessage {
    NewChannel(TX, bool),
    SetChannelId(usize),
    WorkStart(Submission, TestCase),
    Refuse(Submission, TestCase),
    ReJudge(Submission, TestCase),
    WorkDone(
        usize,
        Submission,
        TestCase,
        TestCaseJudgeResult,
        TestCaseJudgeResultInner,
    ),
    Shutdown(usize, Option<Submission>, Option<TestCase>),
}
struct Channel {
    channel_id: usize,
    tx: TX,
    is_working: bool,
    is_precise_measurement: bool,
    current_submission_id: Option<usize>,
}

type TX = tokio::sync::mpsc::Sender<ChannelMessage>;
type RX = tokio::sync::mpsc::Receiver<ChannelMessage>;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(main_async());
}
async fn main_async() {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));

    let mut channels: Vec<Channel> = vec![];
    let mut channel_id: usize = 1;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChannelMessage>(128);

    let mut task_manager = task_manager::TaskManager::new();

    tokio::spawn(listener::Listener::new(tx).run());

    loop {
        tokio::select! {
            Some(rx_msg) = rx.recv() => {
                eprintln!("Received message: {:?}", rx_msg);
                match rx_msg {
                    ChannelMessage::NewChannel(tx, is_precise_measurement) => {
                        tx.send(ChannelMessage::SetChannelId(channel_id)).await.unwrap();

                        channels.push(Channel {
                            channel_id,
                            tx,
                            is_working: false,
                            is_precise_measurement,
                            current_submission_id: None,
                        });

                        channel_id += 1;
                    }
                    ChannelMessage::WorkDone(channel_id, submission, _testcase, result, result_inner) => {
                        if let Some(channel) = channels.iter_mut().find(|channel| channel.channel_id == channel_id) {
                            channel.is_working = false;

                            task_manager.add_result(submission.id, result, result_inner).await;
                        }
                    }
                    ChannelMessage::Shutdown(channel_id, submission, testcase) => {
                        if let Some(pos) = channels.iter().position(|channel| channel.channel_id == channel_id) {
                            channels.remove(pos);

                            if let (Some(submission), Some(testcase)) = (submission, testcase) {
                                task_manager.force_rejudge(submission, testcase);
                            }
                        }
                    }
                    ChannelMessage::Refuse(submission, testcase) => {
                        task_manager.force_rejudge(submission, testcase);
                    }
                    ChannelMessage::ReJudge(submission, testcase) => {
                        task_manager.force_rejudge(submission, testcase);
                    }
                    _ => ()
                }
            }
            _ = interval.tick() => { // 0.1초마다 큐 채우기
                println!("** last task manager {:?}", task_manager.task_quick);


                // early-return 상황이 있을수 있어서 우선 검사
                task_manager.process().await;

                // Testcase는 상황에 따라서 실시간 수정 될 수도 있음. 그렇기 때문에, 루프 안에서만 캐싱 되도록 함
                let (mut available_precise, mut available_quick): (Vec<_>, Vec<_>) = channels.iter_mut().filter(|channel| !channel.is_working).partition(|channel| channel.is_precise_measurement);
                if available_precise.is_empty() && available_quick.is_empty() {
                    continue;
                }

                let (mut task_precise, mut task_quick) = db::list_submissions(available_precise.len().max(8), available_quick.len().max(8)).await;
                if (task_precise.len() + task_quick.len()) > 0 {
                    let mut queued_mark: Vec<i32> = Vec::with_capacity(available_precise.len() + available_quick.len());
                    for task in task_precise.drain(..) {
                        queued_mark.push(task.id);
                        task_manager.add_submissions(task).await;
                    }
                    for task in task_quick.drain(..) {
                        queued_mark.push(task.id);
                        task_manager.add_submissions(task).await;
                    }

                    db::mark_submission_queued(queued_mark).await;
                }

                // starving이 생기지 않도록 두개 다 들고오는 처리

                task_manager.process().await;

                // 특정 서버에서만 (id가 낮은 서버) 작동되지 않도록 - 모든 서버에서 작동되도록 셔플
                available_precise.shuffle(&mut thread_rng());
                available_quick.shuffle(&mut thread_rng());

                let mut redo_precise = Vec::with_capacity(8);
                let mut redo_quick = Vec::with_capacity(8);

                while let Some(task) = task_manager.task_precise.pop() {
                    if let Some(channel) = available_precise.pop() {
                        channel.is_working = true;

                        if (channel.tx.send(ChannelMessage::WorkStart(task.0.clone(), task.1.clone())).await).is_err() {
                            redo_precise.push((task.0.clone(), task.1.clone()));
                        }
                    } else {
                        redo_precise.push((task.0.clone(), task.1.clone()));
                        break;
                    }
                }
                while let Some(task) = task_manager.task_quick.pop() {
                    if let Some(channel) = available_quick.pop().or(available_precise.pop()) {
                        channel.is_working = true;

                        if (channel.tx.send(ChannelMessage::WorkStart(task.0.clone(), task.1.clone())).await).is_err() {
                            redo_quick.push((task.0.clone(), task.1.clone()));
                        }
                    } else {
                        redo_quick.push((task.0.clone(), task.1.clone()));
                        break;
                    }
                }

                println!("redo quick {:?}", redo_quick);
                redo_precise.drain(..).for_each(|(submission, testcase)| task_manager.force_rejudge(submission, testcase));
                redo_quick.drain(..).for_each(|(submission, testcase)| task_manager.force_rejudge(submission, testcase));
            }
        }
    }
}
