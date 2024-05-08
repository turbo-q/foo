// code 使用vscode 打开模糊查找
// dep:
//  dialoguer = "0.11.0"
//  regex = "1.10.4"

use dialoguer::Select;
use regex::Regex;
use std::{
    env,
    path::Path,
    process::{Command, Stdio},
};

/// 执行外部命令并返回执行结果
fn execute_command(command: &str, args: &[&str]) -> Result<String, std::io::Error> {
    let output = Command::new(command).args(args).output()?;

    // 将命令输出转换为字符串
    let output_str = String::from_utf8_lossy(&output.stdout).into_owned();

    Ok(output_str)
}

fn execute_command2(command: &str, args: &[&str]) -> Command {
    let mut command = Command::new(command);
    command.args(args);
    command
}

fn is_substring(main_string: &str, sub_string: &str) -> bool {
    let regex = Regex::new(&format!(".*{}.*", sub_string)).unwrap();
    regex.is_match(main_string)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        panic!("填入项目名,example code project_name")
    }

    let fuzzy_name = args.get(1).expect("获取项目名错误");

    // find 命令标准输出放到管道中
    let find_command = execute_command2(
        "find",
        &["/Users/wangyq/knownsec/project", "-maxdepth", "2"],
    )
    .stdout(Stdio::piped())
    .spawn()
    .expect("find 命令创建失败");

    // grep 命令模糊查找
    let grep_command = execute_command2("grep", &[fuzzy_name])
        .stdin(find_command.stdout.unwrap())
        .output()
        .expect("Failed to execute grep command");
    let err_str = String::from_utf8_lossy(&grep_command.stderr).into_owned();
    if err_str != "" {
        println!("{}", err_str);
        return;
    }
    let output_str = String::from_utf8_lossy(&grep_command.stdout).into_owned();

    // project select
    let mut projects: Vec<&str> = output_str.split_whitespace().collect();
    if projects.len() == 0 {
        println!("NOT FOUND!");
        return;
    }
    // 遍历，只要ext是fuzzy_name 的
    let mut tmp_projects: Vec<&str> = Vec::new();
    for project in projects {
        let p = Path::new(project);
        if let Some(extension) = p.file_name() {
            let s = extension.to_str().unwrap_or_default();
            if is_substring(s, fuzzy_name) {
                tmp_projects.push(project);
            }
        }
    }
    projects = tmp_projects;

    let project: &str;
    if projects.len() == 0 {
        println!("NOT FOUND!");
        return;
    }
    if projects.len() == 1 {
        project = projects[0]
    } else {
        let selection = Select::new()
            .items(&projects)
            .with_prompt("项目选择")
            .interact()
            .unwrap();
        project = projects[selection];
    }

    let _ = execute_command("/usr/local/bin/code", &[project]);
}
