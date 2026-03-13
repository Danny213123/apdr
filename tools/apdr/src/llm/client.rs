use std::process::Command;

#[derive(Clone, Debug)]
pub struct LlmClient {
    pub provider: String,
    pub model: String,
    pub base_url: String,
}

impl LlmClient {
    pub fn new(provider: &str, model: &str, base_url: &str) -> Self {
        Self {
            provider: provider.to_string(),
            model: model.to_string(),
            base_url: base_url.to_string(),
        }
    }

    pub fn is_available(&self) -> bool {
        if self.provider != "ollama" {
            return false;
        }
        Command::new("ollama")
            .arg("show")
            .arg(&self.model)
            .env("OLLAMA_HOST", &self.base_url)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    pub fn complete(&self, prompt: &str) -> Option<String> {
        if self.provider != "ollama" {
            return None;
        }
        let output = Command::new("ollama")
            .arg("run")
            .arg(&self.model)
            .arg(prompt)
            .env("OLLAMA_HOST", &self.base_url)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            None
        } else {
            Some(stdout)
        }
    }
}
