use crate::types::*;
use mysql_async::prelude::*;

use mysql_async::Row;

pub async fn list_submissions(
    precise_avail: usize,
    quick_avail: usize,
) -> (Vec<Submission>, Vec<Submission>) {
    // 굳이 이렇게 나눈 이유는...
    // 원래는 정밀채점이 type = 1이라서 ORDER BY type DESC로 하려 했음.
    // 그런데 생각해 보니까, "정밀 채점"이 시간이 오래 걸려서 100개씩 앞에서 대기가 걸릴수도 있을거라 생각됐음
    // 그래서 강제로 (빠른 채점, 정밀 채점)으로 나눠서 하려는게 목적임
    let precise_result = query(&format!(
        "SELECT * FROM Submit WHERE queued = 0 AND `type` = 1 ORDER BY id LIMIT {}",
        precise_avail
    ))
    .await
    .iter()
    .filter_map(|row| row.clone().try_into().ok())
    .collect::<Vec<_>>();

    let quick_result = query(&format!(
        "SELECT * FROM Submit WHERE queued = 0 AND `type` = 0 ORDER BY id LIMIT {}",
        quick_avail
    ))
    .await
    .iter()
    .filter_map(|row| row.clone().try_into().ok())
    .collect::<Vec<_>>();

    (precise_result, quick_result)
}

pub async fn list_testcase(problem_id: i32) -> Vec<TestCase> {
    let mut testcase = query(
        format!(
            "SELECT * FROM `Testcase` WHERE `problem_id` = {}",
            problem_id,
        )
        .as_str(),
    )
    .await;

    return testcase
        .drain(..)
        .filter_map(|row| row.try_into().ok())
        .collect();
}

pub async fn update_submission_start(id: i32) {
    update_submission_state(id, SubmissionState::InProgress).await;
}

pub async fn update_submission_end(
    submission: &Submission,
    result: bool,
    extra: String,
    memory: usize,
    runtime: usize,
) {
  // println!("update_submission");

    let submit_at = submission.submit_at.format("%Y-%m-%d %T");
    let score = query(&format!("
    SELECT 
        count(*) as tries,
        (SELECT TIMESTAMPDIFF(SECOND, STR_TO_DATE(val, '%Y-%m-%d %H:%i:%s'), STR_TO_DATE('{}', '%Y-%m-%d %H:%i:%s')) FROM config WHERE `key` = 'START_AT') as sec_diff
    FROM Submit WHERE stud_id = {} AND type = 1 AND problemNo = {} AND result = 0
    AND (id < (SELECT min(`id`) FROM Submit WHERE stud_id = {} AND type = 1 AND problemNo = {} AND result = 1))
    ", 
        submit_at, 
        submission.stud_id, submission.problem_no, submission.stud_id, submission.problem_no)
    ).await.pop().unwrap();

    let retries: usize = score.get("tries").unwrap_or(0);
    let secs: usize = score.get("sec_diff").unwrap_or(0);
    let score = if result {
        retries * 20 + (secs / 60)
    } else {
        0
    };

    query(&format!(
            "UPDATE Submit SET score = {}, result = {}, extra = '{}', memory = {}, runtime = {}, state = 2 WHERE id = {}",
            score,
            if result { 0 } else { 1 },
            extra,
            memory,
            runtime,
            submission.id
        )
    )
    .await;

    update_user_problem_stat(submission.stud_id, submission.problem_no, score).await;
}

pub async fn mark_submission_queued(ids: Vec<i32>) {
    if ids.is_empty() {
        return;
    }

    query(&format!(
        "UPDATE Submit SET queued = 1 WHERE id IN ({})",
        ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    ))
    .await;
}

/*
-- ksjudge.Testcase_judge definition

CREATE TABLE `Testcase_judge` (
  `id` int NOT NULL AUTO_INCREMENT,
  `submit_id` int NOT NULL,
  `testcase_id` int NOT NULL,
  `output` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `runtime` int DEFAULT NULL,
  `result` tinyint DEFAULT NULL COMMENT '0 = 성공, 1 = 실패',
  `compile_log` text,
  `memory` int DEFAULT NULL,
  `judge_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `judge_server_id` int NOT NULL DEFAULT '-1',
  `result_extra` varchar(128),
  PRIMARY KEY (`id`),
  KEY `Testcase_judge_FK` (`submit_id`) USING BTREE,
  KEY `Testcase_judge_testcase_FK` (`testcase_id`),
  CONSTRAINT `Testcase_judge_submit_FK` FOREIGN KEY (`submit_id`) REFERENCES `Submit` (`id`) ON UPDATE CASCADE,
  CONSTRAINT `Testcase_judge_testcase_FK` FOREIGN KEY (`testcase_id`) REFERENCES `Testcase` (`id`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 */
pub async fn insert_testcase_judge(
    submission_id: i32,
    testcase_id: i32,
    result: &TestCaseJudgeResult,
    result_inner: &TestCaseJudgeResultInner,
) {
  // println!(
        // "* insert testcase judge: {} {} {:?}",
    //     submission_id, testcase_id, result_inner
    // );
    prepared_query(
        "INSERT INTO Testcase_judge (submit_id, testcase_id, output, runtime, result, compile_log, memory, judge_server_id, result_extra) VALUES (:submit_id, :testcase_id, :output, :runtime, :result, :compile_log, :memory, :judge_server_id, :result_extra)",
        params!{
            "submit_id" => submission_id,
            "testcase_id" => testcase_id,
            "output" => result.output.clone(),
            "runtime" => result.runtime,
            "result" => if result.result { 0 } else { 1 },
            "compile_log" => result.compile_log.clone().unwrap_or("".to_string()),
            "memory" => result.memory.unwrap_or(0) as i64,
            "judge_server_id" => result.judge_server_id.clone(),
            "result_extra" => result_inner.to_string(),
        }
    ).await;
}

async fn update_submission_state(id: i32, state: SubmissionState) {
    query(&format!(
        "UPDATE Submit SET state = {} WHERE id = {}",
        state as i32, id
    ))
    .await;
}

async fn update_user_problem_stat(stud_id: i32, problem_no: i32, score: usize) {
    query(&format!(
        "UPDATE user_problem_stat SET score = {} WHERE stud_id = {} AND problem_no = {}",
        score, stud_id, problem_no
    )).await;
}

async fn query(sql: &str) -> Vec<Row> {
    let url = "mysql://ksu:Rhdqngofk5140%21%40%23@100.82.35.142:3306/ksjudge";
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await.unwrap();

  // println!("query = {}", sql);
    match conn.query(sql).await {
        Err(e) => { eprintln!("error while query: {:?}", e); Vec::new()},
        Ok(val) => {
          // println!("result {:?}", val);
            val
        }
    }
}
async fn prepared_query(sql: &str, params: mysql_async::Params) {
    let url = "mysql://ksu:Rhdqngofk5140%21%40%23@100.82.35.142:3306/ksjudge";
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool.get_conn().await.unwrap(); 

    match sql.with([params]).batch(&mut conn).await {
        Err(e) => { eprintln!("error while prepared query: {:?}", e); },
        Ok(_val) => {
          // println!("result {:?}", val);
        }
    }
}
