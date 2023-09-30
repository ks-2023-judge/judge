use mysql_async::{from_value_opt, FromValueError, Value};
use mysql_async::{prelude::*, Row};

/*
-- ksjudge.Submit definition

CREATE TABLE `Submit` (
  `id` int NOT NULL AUTO_INCREMENT,
  `stud_id` int NOT NULL,
  `type` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '0 = 실행, 1 = 제출',
  `problemNo` int NOT NULL,
  `lang` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `code` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `state` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '0 = 채점 중, 1 = 채점 완료',
  `extra` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `result` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '0 = 정답, 1 = 오답',
  `code_size` int DEFAULT NULL,
  `submit_at` datetime DEFAULT NULL,
  `runtime` int DEFAULT NULL,
  `memory` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Submit_FK` (`stud_id`) USING BTREE,
  KEY `Submit_FK_1` (`problemNo`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
*/
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Submission {
    pub id: i32,
    pub stud_id: i32,
    pub run_type: SubmissionType,
    pub problem_no: i32,
    pub lang: SubmissionLanguage,
    pub code: String,
    pub state: SubmissionState,
    pub extra: Option<String>,
    pub result: Option<SubmissionResult>,
    pub runtime: i32,
    pub memory: i32,
    pub score: Option<u32>,
}
impl Submission {
    pub fn is_precise(&self) -> bool {
        self.run_type == SubmissionType::Precise
    }
}

impl TryFrom<Row> for Submission {
    type Error = ();

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Submission {
            id: row.get("id").ok_or(())?,
            stud_id: row.get("stud_id").ok_or(())?,
            run_type: match row.get("type").ok_or(())? {
                0 => crate::types::SubmissionType::Quick,
                1 => crate::types::SubmissionType::Precise,
                _ => panic!("Invalid submission type"),
            },
            problem_no: row.get("problemNo").ok_or(())?,
            lang: row.get("lang").ok_or(())?,
            code: row.get("code").ok_or(())?,
            state: row.get("state").ok_or(())?,
            extra: row.get("extra").ok_or(())?,
            result: row.get("result").ok_or(())?,
            runtime: row.get("runtime").ok_or(())?,
            memory: row.get("memory").ok_or(())?,
            score: row.get("score").ok_or(())?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionType {
    Precise, // 정밀 채점용
    Quick,   // "실행"버튼 눌렸을때의 평가 (대충 되는지 안되는지 평가용)
}
impl TryFrom<Value> for SubmissionType {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let val = match value {
            mysql_async::Value::Int(val) => val,
            mysql_async::Value::UInt(val) => val as i64,
            _ => 0,
        };

        Ok(match val {
            0 => SubmissionType::Precise,
            1 => SubmissionType::Quick,
            _ => SubmissionType::Precise,
        })
    }
}
impl FromValue for SubmissionType {
    type Intermediate = SubmissionType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionState {
    Submitted,
    InProgress,
    Done,
}
impl TryFrom<Value> for SubmissionState {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let val = match value {
            mysql_async::Value::Int(val) => val,
            mysql_async::Value::UInt(val) => val as i64,
            _ => 0,
        };

        Ok(match val {
            0 => SubmissionState::Submitted,
            1 => SubmissionState::InProgress,
            2 => SubmissionState::Done,
            _ => SubmissionState::Submitted,
        })
    }
}
impl FromValue for SubmissionState {
    type Intermediate = SubmissionState;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionResult {
    Correct,
    Wrong,
}
impl TryFrom<Value> for SubmissionResult {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let val = match value {
            mysql_async::Value::Int(val) => val,
            mysql_async::Value::UInt(val) => val as i64,
            _ => 0,
        };

        Ok(match val {
            0 => SubmissionResult::Correct,
            1 => SubmissionResult::Wrong,
            _ => SubmissionResult::Correct,
        })
    }
}
impl FromValue for SubmissionResult {
    type Intermediate = SubmissionResult;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionLanguage {
    C,
    Cpp,
    Java,
    Python,
    Rust,
    Javascript,
}
impl TryFrom<Value> for SubmissionLanguage {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let lang: String = from_value_opt::<String>(value.clone())?;

        match lang.as_str() {
            "c" => Ok(SubmissionLanguage::C),
            "cpp" => Ok(SubmissionLanguage::Cpp),
            "java" => Ok(SubmissionLanguage::Java),
            "python" => Ok(SubmissionLanguage::Python),
            "rust" => Ok(SubmissionLanguage::Rust),
            "javascript" => Ok(SubmissionLanguage::Javascript),
            _ => Err(FromValueError(value)),
        }
    }
}
impl FromValue for SubmissionLanguage {
    type Intermediate = SubmissionLanguage;
}
impl TryFrom<String> for SubmissionLanguage {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "c" => Ok(SubmissionLanguage::C),
            "cpp" => Ok(SubmissionLanguage::Cpp),
            "java" => Ok(SubmissionLanguage::Java),
            "python" => Ok(SubmissionLanguage::Python),
            "rust" => Ok(SubmissionLanguage::Rust),
            "javascript" => Ok(SubmissionLanguage::Javascript),
            _ => Err(()),
        }
    }
}
impl From<SubmissionLanguage> for String {
    fn from(val: SubmissionLanguage) -> Self {
        match val {
            SubmissionLanguage::C => "c".to_string(),
            SubmissionLanguage::Cpp => "cpp".to_string(),
            SubmissionLanguage::Java => "java".to_string(),
            SubmissionLanguage::Python => "python".to_string(),
            SubmissionLanguage::Rust => "rust".to_string(),
            SubmissionLanguage::Javascript => "javascript".to_string(),
        }
    }
}

/*
CREATE TABLE `Testcase` (
  `id` int NOT NULL AUTO_INCREMENT,
  `input` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `output` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `problem_id` int NOT NULL,
  `isPublic` tinyint NOT NULL DEFAULT '1' COMMENT '0 = 공개, 1 = 비공개',
  `runtime` int DEFAULT NULL COMMENT COMMENT 'ms 단위',
  `memory_limit` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Testcase_FK_1` (`problem_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
*/
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TestCase {
    pub id: i32,
    pub input: String,
    pub output: String,
    pub problem_id: i32,
    pub is_public: bool,
    pub runtime: Option<usize>,
    pub memory_limit: Option<usize>,
}
impl TryFrom<Row> for TestCase {
    type Error = ();

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(TestCase {
            id: row.get("id").ok_or(())?,
            input: row.get("input").ok_or(())?,
            output: row.get("output").ok_or(())?,
            problem_id: row.get("problem_id").ok_or(())?,
            is_public: row.get("isPublic").ok_or(())?,
            runtime: row.get("runtime").ok_or(())?,
            memory_limit: row.get("memory_limit").ok_or(())?,
        })
    }
}

/*
-- ksjudge.Testcase_judge definition

CREATE TABLE `Testcase_judge` (
  `id` int NOT NULL AUTO_INCREMENT,
  `submit_id` int NOT NULL,
  `output` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `runtime` int DEFAULT NULL,
  `result` tinyint DEFAULT NULL COMMENT '0 = 성공, 1 = 실패',
  `compile_log` text,
  PRIMARY KEY (`id`),
  KEY `Testcase_judge_FK` (`submit_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
*/
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TestCaseJudgeResult {
    pub id: Option<i32>,
    pub submit_id: i32,
    pub testcase_id: i32,
    pub output: Option<String>,
    pub runtime: Option<usize>,
    pub memory: Option<usize>,
    pub result: bool,
    pub compile_log: Option<String>,
    pub judge_server_id: String,
}
impl TestCaseJudgeResult {
    pub fn new(
        submit_id: i32,
        testcase_id: i32,
        result: bool,
        output: Option<String>,
        runtime: Option<usize>,
        memory: Option<usize>,
        compile_log: Option<String>,
        judge_server_id: String,
    ) -> Self {
        Self {
            id: None,
            submit_id,
            testcase_id,
            output,
            runtime,
            memory,
            result,
            compile_log,
            judge_server_id,
        }
    }
    pub fn is_correct(&self) -> bool {
        self.result
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum TestCaseJudgeResultInner {
    NotYetDone,
    Accepted,
    TimeLimitExceeded,
    MemoryLimitExceeded,
    WrongAnswer,
    OutputLimitExceeded,
    CompileFailed,
}
impl ToString for TestCaseJudgeResultInner {
    fn to_string(&self) -> String {
        match self {
            Self::NotYetDone => "채점 대기중".to_string(),
            Self::Accepted => "정답".to_string(),
            Self::WrongAnswer => "잘못된 출력".to_string(),
            Self::TimeLimitExceeded => "시간 초과".to_string(),
            Self::MemoryLimitExceeded => "메모리 초과".to_string(),
            Self::OutputLimitExceeded => "출력 초과".to_string(),
            Self::CompileFailed => "컴파일 실패".to_string(),
        }
    }
}
impl From<TestCaseJudgeResultInner> for String {
    fn from(val: TestCaseJudgeResultInner) -> Self {
        match val {
            TestCaseJudgeResultInner::NotYetDone => "NotYetDone".to_string(),
            TestCaseJudgeResultInner::Accepted => "Accepted".to_string(),
            TestCaseJudgeResultInner::WrongAnswer => "WrongAnswer".to_string(),
            TestCaseJudgeResultInner::TimeLimitExceeded => "TimeLimitExceeded".to_string(),
            TestCaseJudgeResultInner::MemoryLimitExceeded => "MemoryLimitExceeded".to_string(),
            TestCaseJudgeResultInner::OutputLimitExceeded => "OutputLimitExceeded".to_string(),
            TestCaseJudgeResultInner::CompileFailed => "CompileFailed".to_string(),
        }
    }
}
impl From<String> for TestCaseJudgeResultInner {
    fn from(val: String) -> Self {
        match val.as_str() {
            "NotYetDone" => TestCaseJudgeResultInner::NotYetDone,
            "Accepted" => TestCaseJudgeResultInner::Accepted,
            "WrongAnswer" => TestCaseJudgeResultInner::WrongAnswer,
            "TimeLimitExceeded" => TestCaseJudgeResultInner::TimeLimitExceeded,
            "MemoryLimitExceeded" => TestCaseJudgeResultInner::MemoryLimitExceeded,
            "OutputLimitExceeded" => TestCaseJudgeResultInner::OutputLimitExceeded,
            "CompileFailed" => TestCaseJudgeResultInner::CompileFailed,
            _ => panic!("Invalid TestCaseJudgeResultInner"),
        }
    }
}
