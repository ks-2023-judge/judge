use bytes::*;

use crate::types::*;

#[derive(Clone, Debug)]
pub struct MessageBody {
    /// len = (len(u64) + m_type(u32) + data_len)
    len: u64,
    m_type: MessageType,

    data: MessageData,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Register(MsgRegister),
    SetTask(MsgSetTask),
    SetTaskAck(MsgSetTaskAck),
    ResultSuccess(MsgResultSuccess),
    ResultFailed(MsgResultFailed),
    Shutdown(MsgShutdown),
    Reset(MsgReset),
}
impl TryInto<Message> for MessageBody {
    type Error = ();
    fn try_into(self) -> Result<Message, ()> {
        match self.m_type {
            MessageType::Register => self.try_into().map(Message::Register),
            MessageType::SetTask => self.try_into().map(Message::SetTask),
            MessageType::SetTaskAck => self.try_into().map(Message::SetTaskAck),
            MessageType::ResultSuccess => self.try_into().map(Message::ResultSuccess),
            MessageType::ResultFailed => self.try_into().map(Message::ResultFailed),
            MessageType::Shutdown => self.try_into().map(Message::Shutdown),
            MessageType::Reset => self.try_into().map(Message::Reset),
        }
    }
}
impl From<Message> for MessageBody {
    fn from(val: Message) -> Self {
        match val {
            Message::Register(msg) => msg.into(),
            Message::SetTask(msg) => msg.into(),
            Message::SetTaskAck(msg) => msg.into(),
            Message::ResultSuccess(msg) => msg.into(),
            Message::ResultFailed(msg) => msg.into(),
            Message::Shutdown(msg) => msg.into(),
            Message::Reset(msg) => msg.into(),
        }
    }
}

#[derive(Clone, Debug)]
enum MessageType {
    Register,
    SetTask,
    SetTaskAck,
    ResultSuccess,
    ResultFailed,
    Shutdown,
    Reset,
}
impl From<MessageType> for u32 {
    fn from(val: MessageType) -> Self {
        match val {
            MessageType::Register => 0,
            MessageType::SetTask => 1,
            MessageType::SetTaskAck => 2,
            MessageType::ResultSuccess => 3,
            MessageType::ResultFailed => 4,
            MessageType::Shutdown => 5,
            MessageType::Reset => 6,
        }
    }
}
impl From<u32> for MessageType {
    fn from(val: u32) -> Self {
        match val {
            0 => MessageType::Register,
            1 => MessageType::SetTask,
            2 => MessageType::SetTaskAck,
            3 => MessageType::ResultSuccess,
            4 => MessageType::ResultFailed,
            5 => MessageType::Shutdown,
            6 => MessageType::Reset,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageData {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<MessageData>),
    UInteger(u64),
    None,
}
impl MessageData {
    pub fn calc_byte_size(&self) -> u64 {
        match self {
            MessageData::String(s) => 4 + 8 + s.len() as u64,
            MessageData::Integer(_) => 4 + 8,
            MessageData::UInteger(_) => 4 + 8,
            MessageData::Float(_) => 4 + 8,
            MessageData::Boolean(_) => 4 + 1,
            MessageData::Array(data) => {
                let mut size = 4;
                for d in data {
                    size += d.calc_byte_size();
                }
                size + 4
            }
            MessageData::None => 4,
        }
    }
}

impl MessageBody {
    pub fn encode(self, buf: &mut BytesMut) {
        buf.put_u64(self.len);
        buf.put_u32(self.m_type.into());

        Self::encode_data(self.data, buf);
    }

    pub fn decode_buf(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }

        // buf에서 len이 충분하지 않을 수 있어서 peek 함
        // buf.len() < len -> 건너뛰기
        let frame_len = u64::from_be_bytes(buf[0..8].try_into().unwrap());
        // eprintln!("decode - len: {} / {}", frame_len, buf.len());

        if (buf.len() as u64) < frame_len {
            return None;
        }

        // peek 한 만큼 advance (len 자체는 차감 하지 않았음)
        buf.advance(8);

        let m_type = MessageType::from(buf.get_u32());

        let mut guard_len = frame_len - 12;

        match Self::decode_data(buf, &mut guard_len) {
            Ok(data) => Some(MessageBody {
                len: frame_len,
                m_type,
                data,
            }),
            Err(remain) => {
                buf.advance(remain as usize);
                None
            }
        }
    }

    fn encode_data(data: MessageData, buf: &mut BytesMut) {
        match data {
            MessageData::String(s) => {
                buf.put_u32(1);
                buf.put_u64(s.len() as u64);
                buf.put(s.as_bytes());
            }
            MessageData::Integer(i) => {
                buf.put_u32(2);
                buf.put_i64(i);
            }
            MessageData::Float(f) => {
                buf.put_u32(3);
                buf.put_f64(f);
            }
            MessageData::Boolean(b) => {
                buf.put_u32(4);
                buf.put_u8(if b { 1 } else { 0 });
            }
            MessageData::Array(data) => {
                buf.put_u32(5);
                buf.put_u32(data.len() as u32);
                for d in data {
                    Self::encode_data(d, buf);
                }
            }
            MessageData::UInteger(i) => {
                buf.put_u32(6);
                buf.put_u64(i);
            }
            MessageData::None => {
                buf.put_u32(7);
                buf.put_u32(0);
            }
        }
    }

    fn decode_data(buf: &mut BytesMut, buf_len_guard: &mut u64) -> Result<MessageData, u64> {
        if *buf_len_guard < 4 {
            // decode시 최소 4byte 이상 남아 있어야 함
            return Err(*buf_len_guard);
        }

        let data_type = buf.get_u32();
        // eprintln!("data type: {}", data_type);
        *buf_len_guard -= 4;

        return match data_type {
            1 => {
                if *buf_len_guard < 8 {
                    return Err(*buf_len_guard);
                }
                let len = buf.get_u64() as usize;

                if *buf_len_guard < (len as u64) {
                    return Err(*buf_len_guard);
                }
                let str = String::from_utf8_lossy(&buf[..len]).to_string();
                buf.advance(len);

                // array 안에 있는 타입일 수 있어서... 정확히 하기 위해 guard 뺄셈 해야 함.
                *buf_len_guard -= 8;
                Ok(MessageData::String(str))
            }
            2 => {
                if *buf_len_guard < 4 {
                    return Err(*buf_len_guard);
                }

                let i = buf.get_i64();

                // array 안에 있는 타입일 수 있어서... 정확히 하기 위해 guard 뺄셈 해야 함.
                *buf_len_guard -= 8;

                Ok(MessageData::Integer(i))
            }
            3 => {
                if *buf_len_guard < 4 {
                    return Err(*buf_len_guard);
                }

                let f = buf.get_f64();

                // array 안에 있는 타입일 수 있어서... 정확히 하기 위해 guard 뺄셈 해야 함.
                *buf_len_guard -= 8;

                Ok(MessageData::Float(f))
            }
            4 => {
                if *buf_len_guard < 1 {
                    return Err(*buf_len_guard);
                }

                let b = buf.get_u8() > 0;

                // array 안에 있는 타입일 수 있어서... 정확히 하기 위해 guard 뺄셈 해야 함.
                *buf_len_guard -= 1;

                Ok(MessageData::Boolean(b))
            }
            5 => {
                if *buf_len_guard < 4 {
                    return Err(*buf_len_guard);
                }

                let len = buf.get_u32() as usize;
                *buf_len_guard -= 4;

                let mut data = Vec::new();
                for _ in 0..len {
                    if let Ok(d) = Self::decode_data(buf, buf_len_guard) {
                        data.push(d);
                    } else {
                        return Err(*buf_len_guard);
                    }
                }

                Ok(MessageData::Array(data))
            }
            6 => {
                if *buf_len_guard < 8 {
                    return Err(*buf_len_guard);
                }

                let i = buf.get_u64();

                *buf_len_guard -= 8;
                Ok(MessageData::UInteger(i))
            }
            7 => Ok(MessageData::None),
            _ => unreachable!(),
        };
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgRegister {
    pub is_precise_server: bool,
}
impl From<MsgRegister> for MessageBody {
    fn from(val: MsgRegister) -> Self {
        let data = MessageData::Boolean(val.is_precise_server);
        Self {
            len: 8 + 4 + data.calc_byte_size(),
            m_type: MessageType::Register,
            data,
        }
    }
}
impl TryFrom<MessageBody> for MsgRegister {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgRegister, ()> {
        if msg.len < 8 + 4 + 1 {
            return Err(());
        }

        match msg.data {
            MessageData::Boolean(b) => Ok(MsgRegister {
                is_precise_server: b,
            }),
            _ => Err(()),
        }
    }
}
impl From<MsgRegister> for Message {
    fn from(val: MsgRegister) -> Self {
        Message::Register(val)
    }
}
impl From<Message> for MsgRegister {
    fn from(val: Message) -> Self {
        match val {
            Message::Register(msg) => msg,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgSetTask {
    pub submission_id: i64,
    pub testcase_id: i64,

    pub lang: SubmissionLanguage,
    pub code: String,
    pub input: String,
    pub expect_output: String,

    pub time_limit: u64,
    pub memory_limit: u64,

    pub is_decimal_mode: bool,
}
impl From<MsgSetTask> for MessageBody {
    fn from(val: MsgSetTask) -> Self {
        let data = MessageData::Array(vec![
            MessageData::Integer(val.submission_id),
            MessageData::Integer(val.testcase_id),
            MessageData::String(val.lang.into()),
            MessageData::String(val.code),
            MessageData::String(val.input),
            MessageData::String(val.expect_output),
            MessageData::UInteger(val.time_limit),
            MessageData::UInteger(val.memory_limit),
            MessageData::Boolean(val.is_decimal_mode),
        ]);
        Self {
            len: 8 + 4 + data.calc_byte_size(),
            m_type: MessageType::SetTask,
            data,
        }
    }
}
impl TryFrom<MessageBody> for MsgSetTask {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgSetTask, ()> {
        if msg.len < 8 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 {
            return Err(());
        }

        match msg.data {
            MessageData::Array(data) => {
                if data.len() < 8 {
                    return Err(());
                }

                let submission_id = if let MessageData::Integer(i) = data[0] {
                    i
                } else {
                    return Err(());
                };

                let testcase_id = if let MessageData::Integer(i) = data[1] {
                    i
                } else {
                    return Err(());
                };

                let lang = if let MessageData::String(s) = data[2].clone() {
                    s
                } else {
                    return Err(());
                };

                let code = if let MessageData::String(s) = data[3].clone() {
                    s
                } else {
                    return Err(());
                };

                let input = if let MessageData::String(s) = data[4].clone() {
                    s
                } else {
                    return Err(());
                };

                let expect_output = if let MessageData::String(s) = data[5].clone() {
                    s
                } else {
                    return Err(());
                };

                let time_limit = if let MessageData::UInteger(i) = data[6] {
                    i
                } else {
                    return Err(());
                };

                let memory_limit = if let MessageData::UInteger(i) = data[7] {
                    i
                } else {
                    return Err(());
                };

                let is_decimal_mode = if let MessageData::Boolean(b) = data[8] {
                    b
                } else {
                    return Err(());
                };

                Ok(MsgSetTask {
                    submission_id,
                    testcase_id,
                    lang: lang.try_into().unwrap(),
                    code,
                    input,
                    expect_output,
                    time_limit,
                    memory_limit,
                    is_decimal_mode,
                })
            }
            _ => Err(()),
        }
    }
}
impl From<MsgSetTask> for Message {
    fn from(val: MsgSetTask) -> Self {
        Message::SetTask(val)
    }
}
impl From<Message> for MsgSetTask {
    fn from(val: Message) -> Self {
        match val {
            Message::SetTask(msg) => msg,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgSetTaskAck {
    pub submission_id: i64,
    pub testcase_id: i64,
}
impl From<MsgSetTaskAck> for MessageBody {
    fn from(val: MsgSetTaskAck) -> Self {
        let data = MessageData::Array(vec![
            MessageData::Integer(val.submission_id),
            MessageData::Integer(val.testcase_id),
        ]);
        Self {
            len: 8 + 4 + data.calc_byte_size(),
            m_type: MessageType::SetTaskAck,
            data,
        }
    }
}
impl TryFrom<MessageBody> for MsgSetTaskAck {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgSetTaskAck, ()> {
        if msg.len < 8 + 4 + 8 + 8 {
            return Err(());
        }

        match msg.data {
            MessageData::Array(data) => {
                if data.len() != 2 {
                    return Err(());
                }

                let submission_id = if let MessageData::Integer(i) = data[0] {
                    i
                } else {
                    return Err(());
                };

                let testcase_id = if let MessageData::Integer(i) = data[1] {
                    i
                } else {
                    return Err(());
                };

                Ok(MsgSetTaskAck {
                    submission_id,
                    testcase_id,
                })
            }
            _ => Err(()),
        }
    }
}
impl From<MsgSetTaskAck> for Message {
    fn from(val: MsgSetTaskAck) -> Self {
        Message::SetTaskAck(val)
    }
}
impl From<Message> for MsgSetTaskAck {
    fn from(val: Message) -> Self {
        match val {
            Message::SetTaskAck(msg) => msg,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgResult {
    pub submission_id: i64,
    pub testcase_id: i64,

    pub output_compile: String,
    pub output_run: String,
    pub result: TestCaseJudgeResultInner,
    pub result_extra: String,

    pub time_used: u64,
    pub memory_used: u64,
    pub judge_server_id: String,
}
impl From<MsgResult> for MessageBody {
    fn from(val: MsgResult) -> Self {
        let m_type = match val.result {
            TestCaseJudgeResultInner::Accepted => MessageType::ResultSuccess,
            _ => MessageType::ResultFailed,
        };

        let data = MessageData::Array(vec![
            MessageData::Integer(val.submission_id),
            MessageData::Integer(val.testcase_id),
            MessageData::String(val.output_compile),
            MessageData::String(val.output_run),
            MessageData::String(val.result.into()),
            MessageData::String(val.result_extra),
            MessageData::UInteger(val.time_used),
            MessageData::UInteger(val.memory_used),
            MessageData::String(val.judge_server_id),
        ]);
        Self {
            len: 8 + 4 + data.calc_byte_size(),
            m_type,
            data,
        }
    }
}
impl TryFrom<MessageBody> for MsgResult {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgResult, ()> {
        if msg.len < 8 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4 {
            return Err(());
        }

        match msg.data {
            MessageData::Array(data) => {
                if data.len() != 9 {
                    return Err(());
                }

                let submission_id = if let MessageData::Integer(i) = data[0] {
                    i
                } else {
                    return Err(());
                };

                let testcase_id = if let MessageData::Integer(i) = data[1] {
                    i
                } else {
                    return Err(());
                };

                let output_compile = if let MessageData::String(s) = data[2].clone() {
                    s
                } else {
                    return Err(());
                };

                let output_run = if let MessageData::String(s) = data[3].clone() {
                    s
                } else {
                    return Err(());
                };

                let result = if let MessageData::String(s) = data[4].clone() {
                    s
                } else {
                    return Err(());
                };

                let result_extra = if let MessageData::String(s) = data[5].clone() {
                    s
                } else {
                    return Err(());
                };

                let time_used = if let MessageData::UInteger(i) = data[6] {
                    i
                } else {
                    return Err(());
                };

                let memory_used = if let MessageData::UInteger(i) = data[7] {
                    i
                } else {
                    return Err(());
                };

                let judge_server_id = if let MessageData::String(s) = data[8].clone() {
                    s
                } else {
                    return Err(());
                };

                Ok(MsgResult {
                    submission_id,
                    testcase_id,
                    output_compile,
                    output_run,
                    result: result.try_into().unwrap(),
                    result_extra,
                    time_used,
                    memory_used,
                    judge_server_id,
                })
            }
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgResultSuccess(pub MsgResult);
impl From<MsgResultSuccess> for MessageBody {
    fn from(val: MsgResultSuccess) -> Self {
        Self::from(val.0)
    }
}
impl TryFrom<MessageBody> for MsgResultSuccess {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgResultSuccess, ()> {
        Ok(MsgResultSuccess(MsgResult::try_from(msg)?))
    }
}
impl From<MsgResultSuccess> for Message {
    fn from(val: MsgResultSuccess) -> Self {
        Message::ResultSuccess(MsgResultSuccess(val.0))
    }
}
impl From<Message> for MsgResultSuccess {
    fn from(val: Message) -> Self {
        match val {
            Message::ResultSuccess(msg) => msg,
            _ => unreachable!(),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct MsgResultFailed(pub MsgResult);
impl From<MsgResultFailed> for MessageBody {
    fn from(val: MsgResultFailed) -> Self {
        Self::from(val.0)
    }
}

impl TryFrom<MessageBody> for MsgResultFailed {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgResultFailed, ()> {
        Ok(MsgResultFailed(MsgResult::try_from(msg)?))
    }
}
impl From<MsgResultFailed> for Message {
    fn from(val: MsgResultFailed) -> Self {
        Message::ResultFailed(MsgResultFailed(val.0))
    }
}
impl From<Message> for MsgResultFailed {
    fn from(val: Message) -> Self {
        match val {
            Message::ResultFailed(msg) => msg,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgShutdown {}
impl From<MsgShutdown> for MessageBody {
    fn from(_val: MsgShutdown) -> Self {
        Self {
            len: 8 + 4,
            m_type: MessageType::Shutdown,
            data: MessageData::None,
        }
    }
}
impl TryFrom<MessageBody> for MsgShutdown {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgShutdown, ()> {
        if msg.len < 8 + 4 {
            return Err(());
        }

        Ok(MsgShutdown {})
    }
}
impl From<MsgShutdown> for Message {
    fn from(val: MsgShutdown) -> Self {
        Message::Shutdown(val)
    }
}
impl From<Message> for MsgShutdown {
    fn from(val: Message) -> Self {
        match val {
            Message::Shutdown(msg) => msg,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MsgReset {}
impl From<MsgReset> for MessageBody {
    fn from(_val: MsgReset) -> Self {
        Self {
            len: 8 + 4,
            m_type: MessageType::Reset,
            data: MessageData::None,
        }
    }
}
impl TryFrom<MessageBody> for MsgReset {
    type Error = ();

    fn try_from(msg: MessageBody) -> Result<MsgReset, ()> {
        if msg.len < 8 + 4 {
            return Err(());
        }

        Ok(MsgReset {})
    }
}
impl From<MsgReset> for Message {
    fn from(val: MsgReset) -> Self {
        Message::Reset(val)
    }
}
impl From<Message> for MsgReset {
    fn from(val: Message) -> Self {
        match val {
            Message::Reset(msg) => msg,
            _ => unreachable!(),
        }
    }
}
