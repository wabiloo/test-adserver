const DBAdapter = require("./db-adapter");
const { PaginateMemoryDB, Transform } = require("../utils/utilities");

SESSION_STORE = {};

class MemoryDBAdapter extends DBAdapter {
  // Add Session obj to storage.
  async AddSessionToStorage(session) {
    SESSION_STORE[session.sessionId] = session;
    return 1;
  }

  // Extract and filter all Session objects
  async getAllSessions(opt) {
    let sessionList = Object.values(SESSION_STORE);

    // Filter session list on host field.
    if (opt && opt.targetHost && opt.targetHost != "") {
      sessionList = sessionList.filter( session => session.host.localeCompare(opt.targetHost) === 0);
    }
     // Sort by newest first
    sessionList.sort((a, b) => {
      const dateA = new Date(a["created"]);
      const dateB = new Date(b["created"]);
      return dateB - dateA;
    });
    
    return sessionList
  }

  // Get a List of running test sessions.
  async getSessions(opt) {
    let sessionList = await this.getAllSessions(opt)

    // Assuming We are to always respond with a pagination.
    // Paginate w/ query params, deafult values used otherwise.
    sessionList = PaginateMemoryDB(sessionList, opt.page, opt.limit);
    sessionList.data = sessionList.data.map((session) => {
      return Transform(session);
    });
    // Return Pagination Object
    return sessionList;
  }

  // Get a List of running test sessions.
  async getSessionsByUserId(userId) {
    let sessionList = Object.values(SESSION_STORE).filter(
      (session) => userId == session.getUser()
    );
    // If empty, then we have no sessions.
    if (sessionList.length === 0) {
      return null;
    }
    // Sort by newest first
    sessionList.sort((a, b) => {
    const dateA = new Date(a["created"]);
    const dateB = new Date(b["created"]);
    return dateB - dateA;
    });
    
    return sessionList;
  }

  // Get information of a specific test session.
  async getSession(sessionId) {
    const session = SESSION_STORE[sessionId];
    return session;
  }

  // Get all events across all sessions.
  async getEvents(opt) {
    const sessionList = await this.getAllSessions(opt)

    let eventsList = sessionList.reduce((events, session) => {
      session.getTrackedEvents().events.forEach( event => {
        events.push(event)
      })
      return events;
    }, []);

      // Sort by newest first
    eventsList.sort((a, b) => {
      const dateA = new Date(a["issuedAt"]);
      const dateB = new Date(b["issuedAt"]);
      return dateB - dateA;
    });

    eventsList = PaginateMemoryDB(eventsList, opt.page, opt.limit);
    
    return eventsList;
  }

  async DeleteSession(sessionId) {
    delete SESSION_STORE[sessionId];
    return 1;
  }
}

const adapter = new MemoryDBAdapter();

module.exports = adapter;
