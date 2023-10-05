use std::collections::HashMap;

use crate::{db, types::*};

#[derive(Debug, Clone)]
enum JudgeState {
    Inqueue,
    InPublic,
    InPrivate,
    Done,
}
enum JudgeAction {
    NoOp,
    /// (result, extra, runtime, memory)
    End(bool, String, usize, usize),
    AddPreciseTestcase(Vec<TestCase>),
    /// (priority, testcases)
    AddQuickTestcase(bool, Vec<TestCase>),
    // listener 쪽에서 300초 hard-limit이 있어서 문제 없을듯
    // Cancel(Submission, TestCase),
}

#[derive(Clone, Debug)]
struct JudgeInfo {
    submission: Submission,
    testcase_public: Vec<TestCase>,
    testcase_private: Vec<TestCase>,

    testcase_result: HashMap<i32, (TestCaseJudgeResult, TestCaseJudgeResultInner)>,

    testcase_public_passed: TestCaseJudgeResultInner,
    testcase_private_passed: TestCaseJudgeResultInner,

    is_start: bool,

    state: JudgeState,
}
impl JudgeInfo {
    pub fn new(submission: Submission, testcase_of_problem: Vec<TestCase>) -> Self {
        let (testcase_pub, testcase_priv) = testcase_of_problem
            .into_iter()
            .partition(|testcase| testcase.is_public);

        Self {
            submission,
            testcase_public: testcase_pub,
            testcase_private: testcase_priv,
            testcase_result: HashMap::new(),

            testcase_public_passed: TestCaseJudgeResultInner::NotYetDone,
            testcase_private_passed: TestCaseJudgeResultInner::NotYetDone,

            is_start: false,

            state: JudgeState::Inqueue,
        }
    }

    pub fn process(&mut self) -> JudgeAction {
        println!("** process");

        match (&self.state, self.submission.run_type) {
            (JudgeState::Inqueue, _) => {
                self.state = JudgeState::InPublic;
                let is_priority = self.submission.run_type == SubmissionType::Precise;

                JudgeAction::AddQuickTestcase(is_priority, self.testcase_public.clone())
            }
            (JudgeState::InPublic, _) => {
                println!(
                    "-- {:?} / {:?}",
                    self.testcase_result.len(),
                    self.testcase_public.len()
                );
                if self.testcase_result.len() < self.testcase_public.len() {
                    return JudgeAction::NoOp;
                }

                // TODO: 실제로 성공했는지 체크 필요

                let is_failed = self
                    .testcase_result
                    .values()
                    .any(|v| !matches!(v.1, TestCaseJudgeResultInner::Accepted));

                let testcase_ids: Vec<_> = self.testcase_public.iter().map(|t| t.id).collect();
                let max_time = self
                    .testcase_result
                    .iter()
                    .filter(|(k, _)| testcase_ids.contains(k))
                    .map(|(_, v)| v.0.runtime.unwrap_or(0))
                    .max()
                    .unwrap_or(0);

                let max_memory = self
                    .testcase_result
                    .iter()
                    .filter(|(k, _)| testcase_ids.contains(k))
                    .map(|(_, v)| v.0.memory.unwrap_or(0))
                    .max()
                    .unwrap_or(0);

                if is_failed || matches!(self.submission.run_type, SubmissionType::Quick) {
                    self.state = JudgeState::Done;

                    let result = self
                        .testcase_result
                        .iter()
                        .filter(|(k, _)| testcase_ids.contains(k))
                        .map(|(_, v)| v.1.clone())
                        .max()
                        .unwrap_or(TestCaseJudgeResultInner::CompileFailed);

                    JudgeAction::End(!is_failed, result.to_string(), max_time, max_memory)
                } else {
                    self.state = JudgeState::InPrivate;
                    JudgeAction::AddPreciseTestcase(self.testcase_private.clone())
                }
            }
            (JudgeState::InPrivate, SubmissionType::Precise) => {
                if self.testcase_result.len()
                    < self.testcase_public.len() + self.testcase_private.len()
                {
                    return JudgeAction::NoOp;
                }

                self.state = JudgeState::Done;
                let _result = self.testcase_private_passed.clone();

                let is_failed = self
                    .testcase_result
                    .values()
                    .any(|v| !matches!(v.1, TestCaseJudgeResultInner::Accepted));

                let max_time = self
                    .testcase_result
                    .values()
                    .map(|v| v.0.runtime.unwrap_or(0))
                    .max()
                    .unwrap_or(0);

                let max_memory = self
                    .testcase_result
                    .values()
                    .map(|v| v.0.memory.unwrap_or(0))
                    .max()
                    .unwrap_or(0);

                let result = self
                    .testcase_result
                    .values()
                    .map(|v| v.1.clone())
                    .max()
                    .unwrap_or(TestCaseJudgeResultInner::CompileFailed);

                self.state = JudgeState::Done;
                JudgeAction::End(!is_failed, result.to_string(), max_time, max_memory)
            }
            _ => JudgeAction::NoOp,
        }
    }
}

pub struct TaskManager {
    pub task_precise: Vec<(Submission, TestCase)>,
    pub task_quick: Vec<(Submission, TestCase)>,

    submissions: HashMap<i32, JudgeInfo>,

    testcase_cache: HashMap<i32, (std::time::SystemTime, Vec<TestCase>)>,
}

impl TaskManager {
    pub fn new() -> TaskManager {
        TaskManager {
            task_precise: Vec::with_capacity(128),
            task_quick: Vec::with_capacity(128),

            submissions: HashMap::<i32, JudgeInfo>::new(),

            testcase_cache: HashMap::new(),
        }
    }

    pub async fn add_submissions(&mut self, submission: Submission) {
        let testcase = self.list_testcase(submission.problem_no).await;

        let judge = JudgeInfo::new(submission, testcase);

        eprintln!("add test {:?}", judge);
        self.submissions.insert(judge.submission.id, judge);
    }
    pub async fn add_result(
        &mut self,
        submission_id: i32,
        result: TestCaseJudgeResult,
        result_inner: TestCaseJudgeResultInner,
    ) {
        db::insert_testcase_judge(submission_id, result.testcase_id, &result, &result_inner).await;

        if let Some(judge) = self.submissions.get_mut(&submission_id) {
            judge
                .testcase_result
                .insert(result.testcase_id, (result, result_inner));
        }
    }

    pub fn force_rejudge(&mut self, submission: Submission, testcase: TestCase) {
        println!("task quick queue: {:?}", self.task_quick);

        // 이미 큐에 있으면 패스함
        match testcase.is_public {
            false => {
                if self
                    .task_precise
                    .iter()
                    .any(|(s, t)| s.id == submission.id && t.id == testcase.id)
                {
                    return;
                }
                self.task_precise.insert(0, (submission, testcase))
            }
            true => {
                if self
                    .task_quick
                    .iter()
                    .any(|(s, t)| s.id == submission.id && t.id == testcase.id)
                {
                    return;
                }
                self.task_quick.insert(0, (submission, testcase))
            }
        }

        println!("--> task quick queue {:?}", self.task_quick);
    }

    pub async fn process(&mut self) {
        let mut actions: Vec<(Submission, JudgeAction)> =
            Vec::with_capacity(self.submissions.len());

        for judge in self.submissions.values_mut() {
            actions.push((judge.submission.clone(), judge.process()));
        }

        for (sub, act) in actions {
            if !(self.process_judge(&sub, act).await) {
                self.submissions.remove(&sub.id);
            }
        }
    }

    async fn process_judge(&mut self, submission: &Submission, judge: JudgeAction) -> bool {
        match judge {
            JudgeAction::AddPreciseTestcase(mut testcases) => {
                self.task_precise
                    .extend(testcases.drain(..).map(|t| (submission.clone(), t)));
            }
            JudgeAction::AddQuickTestcase(is_priority, mut testcases) => {
                if is_priority {
                    testcases.reverse();

                    testcases
                        .drain(..)
                        .for_each(|t| self.task_quick.insert(0, (submission.clone(), t)));
                } else {
                    self.task_quick
                        .extend(testcases.drain(..).map(|t| (submission.clone(), t)));
                }
            }
            JudgeAction::End(result, msg, runtime, memory) => {
                db::update_submission_end(submission, result, msg, memory, runtime).await;

                return false;
            }
            JudgeAction::NoOp => (),
            // JudgeAction::Cancel(submission, testcase) => self.force_rejudge(submission, testcase),
        }

        true
    }

    async fn list_testcase(&mut self, problem_no: i32) -> Vec<TestCase> {
        println!("testcase cache {:?}", self.testcase_cache);

        if let Some((fetch_time, testcase)) = self.testcase_cache.get(&problem_no) {
            if let Ok(is_cache_expired) = fetch_time.elapsed().map(|t| t.as_secs() < 5) {
                println!("is elapsed");
                if !is_cache_expired {
                    return testcase.clone();
                }
            }
        };

        let testcases = db::list_testcase(problem_no).await;
        self.testcase_cache.insert(
            problem_no,
            (std::time::SystemTime::now(), testcases.clone()),
        );

        testcases
    }
}
