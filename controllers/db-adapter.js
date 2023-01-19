class DBAdapter {
  async AddSessionToStorage(session) {}

  async getAllSessions(opt) {}

  async getSessions(opt) {}

  async getSessionsByUserId(userId) {}

  async getSession(sessionId) {}

  async DeleteSession(sessionId) {}

  async getEvents(opt) {}
}

module.exports = DBAdapter;
