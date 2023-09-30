use bytes::*;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{protocol::*, types::*, ChannelMessage, Submission, RX, TX};

pub struct Listener {
    tx_manager: TX,
}
impl Listener {
    pub fn new(tx: TX) -> Self {
        Listener { tx_manager: tx }
    }

    pub async fn run(self) {
        let tcp_listener = tokio::net::TcpListener::bind("0.0.0.0:33333")
            .await
            .unwrap();

        loop {
            if let Ok((tcp_stream, _)) = tcp_listener.accept().await {
                let tx_cloned = self.tx_manager.clone();

                let stream = Stream::new(tx_cloned, tcp_stream);
                tokio::spawn(stream.run());
            }
        }
    }
}

#[derive(Debug)]
pub struct Stream {
    tx_manager: TX,
    stream_id: usize,
    stream: tokio::net::TcpStream,

    recv_buf: BytesMut,
    send_buf: BytesMut,

    current_submission: Option<Submission>,
    current_testcase: Option<TestCase>,

    is_started: bool,
    start_dt: Option<std::time::Instant>,
}
impl Stream {
    pub fn new(tx_manager: TX, stream: tokio::net::TcpStream) -> Self {
        Self {
            tx_manager,
            stream,
            stream_id: 0,

            recv_buf: BytesMut::with_capacity(8192),
            send_buf: BytesMut::with_capacity(8192),

            current_submission: None,
            current_testcase: None,

            is_started: false,
            start_dt: None,
        }
    }

    pub async fn run(mut self) {
        let (stream_tx, mut stream_rx) = tokio::sync::mpsc::channel::<ChannelMessage>(256);

        let msg = self.recv().await;
        println!("handshake {:?}", msg);

        if let Ok(Message::Register(msg)) = msg {
            self.tx_manager
                .send(ChannelMessage::NewChannel(stream_tx, msg.is_precise_server))
                .await
                .unwrap();
        } else {
            return;
        }

        eprintln!("ack completed");

        if let ChannelMessage::SetChannelId(id) = stream_rx.recv().await.unwrap() {
            self.stream_id = id;
        }

        self.judge(stream_rx).await;

        println!("listner loop end - {:?}", self);

        drop(
            self.tx_manager
                .send(ChannelMessage::Shutdown(
                    self.stream_id,
                    self.current_submission.take(),
                    self.current_testcase.take(),
                ))
                .await,
        );
    }

    async fn judge(&mut self, mut stream_rx: RX) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            tokio::select!(
                msg = stream_rx.recv() => {
                    eprintln!("recv on listener {:?}", msg);
                    match msg {
                        Some(ChannelMessage::WorkStart(submission, test_case)) => {
                            if self.current_submission.is_some() {
                                self.tx_manager
                                  .send(ChannelMessage::Refuse(submission, test_case))
                                  .await
                                  .unwrap();

                                continue;
                            }

                            self.current_submission = Some(submission.clone());
                            self.current_testcase = Some(test_case.clone());

                            let msg = Message::SetTask(MsgSetTask {
                                    submission_id: submission.id as i64,
                                    testcase_id: test_case.id as i64,

                                    lang: submission.lang,
                                    code: submission.code,
                                    input: test_case.input,
                                    expect_output: test_case.output,


                                    time_limit: test_case.runtime.unwrap_or(15) as u64,
                                    memory_limit: test_case.memory_limit.unwrap_or(1_000_000_000) as u64,
                            });

                            let body: MessageBody = msg.into();
                            body.encode(&mut self.send_buf);

                            self.stream.write_buf(&mut self.send_buf).await.unwrap();
                            self.start_dt = Some(std::time::Instant::now());
                        }
                        _ => {
                            todo!()
                        }
                    }
                },
                data = self.recv() => {
                    if data.is_err() { return }

                    if (self.process_socket(data.unwrap()).await).is_err() {
                        return;
                    };
                }
                _ = interval.tick(), if self.is_started => {
                    if let Some(dt) = self.start_dt {
                        if dt.elapsed().as_secs() > 180 {
                            // 180초 이상 반응이 없으면 워커가 죽은거로 판단
                            // (위에서 shutdown 처리돼서 rejudge 걸림)
                            return;
                        }
                    }
                }
            )
        }
    }

    async fn process_socket(&mut self, msg: Message) -> Result<(), ()> {
        // 출력
        // 실행 시간
        // 실행 메모리
        // 컴파일 로그
        // 결과

        match msg {
            Message::SetTaskAck(_) => {
                self.is_started = true;
            }
            Message::ResultSuccess(msg) => {
                let msg = msg.0;

                let result = TestCaseJudgeResult::new(
                    self.current_submission.as_ref().unwrap().id,
                    self.current_testcase.as_ref().unwrap().id,
                    true,
                    Some(msg.output_run),
                    Some(msg.time_used as usize),
                    Some(msg.memory_used as usize),
                    Some(msg.output_compile),
                    msg.judge_server_id,
                );
                let result_inner = TestCaseJudgeResultInner::Accepted;

                self.tx_manager
                    .send(ChannelMessage::WorkDone(
                        self.stream_id,
                        self.current_submission.take().unwrap(),
                        self.current_testcase.take().unwrap(),
                        result,
                        result_inner,
                    ))
                    .await
                    .unwrap();
            }
            Message::ResultFailed(msg) => {
                println!("- msg failed {:?}, {:?}", msg, self);

                let msg = msg.0;
                let result = TestCaseJudgeResult::new(
                    self.current_submission.as_ref().unwrap().id,
                    self.current_testcase.as_ref().unwrap().id,
                    false,
                    Some(msg.output_run),
                    Some(msg.time_used as usize),
                    Some(msg.memory_used as usize),
                    Some(msg.output_compile),
                    msg.judge_server_id,
                );
                let result_inner = TestCaseJudgeResultInner::WrongAnswer;

                self.tx_manager
                    .send(ChannelMessage::WorkDone(
                        self.stream_id,
                        self.current_submission.take().unwrap(),
                        self.current_testcase.take().unwrap(),
                        result,
                        result_inner,
                    ))
                    .await
                    .unwrap();
            }
            Message::Shutdown(_) => {
                return Err(());
            }
            Message::Reset(_) => {
                return Err(());
            }
            _ => {}
        };

        Ok(())
    }

    async fn recv(&mut self) -> Result<Message, ()> {
        while let Ok(len) = self.stream.read_buf(&mut self.recv_buf).await {
            if len == 0 {
                return Err(());
            }

            if let Some(msg) = MessageBody::decode_buf(&mut self.recv_buf) {
                eprintln!("recv: {:?}", msg);
                return msg.try_into();
            }
        }

        Err(())
    }
}
