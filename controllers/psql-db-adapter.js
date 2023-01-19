const DBAdapter = require("./db-adapter");

class PsqlDBAdapter extends DBAdapter {
  async AddSessionToStorage(session) {
    // TODO ...
  }

  // Get a list of running test sessions (objects).
  async getAllSessions(opt) {
    // TODO ...
  }

  // Get a list of running test sessions.
  async getSessions(opt) {
    // TODO ...
  }

  // Get a list of running test sessions.
  async getSessionsByUserId(userId) {
    // TODO ...
  }

  // Get information of a specific test session.
  async getSession(sessionId) {
    // TODO ...
  }

  async DeleteSession(sessionId) {
    // TODO ...
  }

  // Get a list of all events across all sessions.
  async getEvents(opt) {
    // TODO ...
  }

}

const adapter = new PsqlDBAdapter();

module.exports = adapter;
