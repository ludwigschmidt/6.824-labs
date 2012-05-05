
				if (first_replica) {
					breakpoint1();
					first_replica = false;
					partition1();
				}
			}
		}

		if (all_ok) {
			execute(procno, req, r);
		} else {
			// printf("returning busy because not all ok\n");
			// fflush(stdout);
			ret = fab_client_protocol::BUSY;
		}
	}

	pthread_mutex_unlock(&fab_mutex);
	// printf("client_invoke returns %d\n", ret);
	// fflush(stdout);
	return ret;
}

// 
// The primary calls the internal invoke at each member of the
// replicated state machine 
//
// the replica must execute requests in order (with no gaps) 
// according to requests' seqno 

fab_protocol::status
fab::invoke(int proc, viewstamp vs, std::string req, int &dummy)
{
	// You fill this in for Lab 7
	ScopedLock ml(&invoke_mutex);
	ScopedLock rl(&fab_mutex);

	fab_protocol::status ret = fab_protocol::OK;

	if (inviewchange) {
		ret = fab_protocol::ERR;
	} else if (primary == cfg->myaddr()) {
		ret = fab_protocol::ERR;
	} else if (vs != myvs) {
		ret = fab_protocol::ERR;
	} else {
		last_myvs = myvs;
		++myvs.seqno;
		std::string r;
		execute(proc, req, r);

		breakpoint1();
	}

	return ret;
}

/**
 * RPC handler: Send back the local node's state to the caller
 */
fab_protocol::status
fab::transferreq(std::string src, viewstamp last, unsigned vid, 
fab_protocol::transferres &r)
{
	ScopedLock ml(&fab_mutex);
	int ret = fab_protocol::OK;
	// Code will be provided in Lab 7
	tprintf("transferreq from %s (%d,%d) vs (%d,%d)\n", src.c_str(), 
	 last.vid, last.seqno, last_myvs.vid, last_myvs.seqno);
	if (!insync || vid != vid_insync) {
		 return fab_protocol::BUSY;
	}
	if (stf && last != last_myvs) 
		r.state = stf->marshal_state();
	r.last = last_myvs;
	return ret;
}

/**
	* RPC handler: Inform the local node (the primary) that node m has synchronized
	* for view vid
	*/
fab_protocol::status
fab::transferdonereq(std::string m, unsigned vid, int &)
{
	int ret = fab_protocol::OK;
	ScopedLock ml(&fab_mutex);
	// You fill this in for Lab 7
	// - Return BUSY if I am not insync, or if the slave is not synchronizing
	//   for the same view with me
	// - Remove the slave from the list of unsynchronized backups
	// - Wake up recovery thread if all backups are synchronized

	if (!insync || vid != vid_insync) {
		ret = fab_protocol::BUSY;
	} else {
		backups.erase(m);
		if (backups.empty()) {
			pthread_cond_broadcast(&recovery_cond);
		}
	}
	return ret;
}

// a node that wants to join an fab as a server sends a
// joinreq to the fab's current primary; this is the
// handler for that RPC.
fab_protocol::status
fab::joinreq(std::string m, viewstamp last, fab_protocol::joinres &r)
{
	int ret = fab_protocol::OK;

	ScopedLock ml(&fab_mutex);
	tprintf("joinreq: src %s last (%d,%d) mylast (%d,%d)\n", m.c_str(), 
	 last.vid, last.seqno, last_myvs.vid, last_myvs.seqno);
	if (cfg->ismember(m, vid_commit)) {
		tprintf("joinreq: is still a member\n");
		r.log = cfg->dump();
	} else if (cfg->myaddr() != primary) {
		tprintf("joinreq: busy\n");
		ret = fab_protocol::BUSY;
	} else {
		// We cache vid_commit to avoid adding m to a view which already contains 
		// m due to race condition
		unsigned vid_cache = vid_commit;
		VERIFY (pthread_mutex_unlock(&fab_mutex) == 0);
		bool succ = cfg->add(m, vid_cache);
		VERIFY (pthread_mutex_lock(&fab_mutex) == 0);
		if (cfg->ismember(m, cfg->vid())) {
			r.log = cfg->dump();
			tprintf("joinreq: ret %d log %s\n:", ret, r.log.c_str());
		} else {
			tprintf("joinreq: failed; proposer couldn't add %d\n", succ);
			ret = fab_protocol::BUSY;
		}
	}
	return ret;
}

/*
 * RPC handler: Send back all the nodes this local knows about to client
 * so the client can switch to a different primary 
 * when it existing primary fails
 */
fab_client_protocol::status
fab::client_members(int i, std::vector<std::string> &r)
{
	std::vector<std::string> m;
	ScopedLock ml(&fab_mutex);
	m = cfg->get_view(vid_commit);
	m.push_back(primary);
	r = m;
	tprintf("fab::client_members return %s m %s\n", print_members(m).c_str(),
	 primary.c_str());
	return fab_client_protocol::OK;
}

// if primary is member of new view, that node is primary
// otherwise, the lowest number node of the previous view.
// caller should hold fab_mutex
void
fab::set_primary(unsigned vid)
{
	std::vector<std::string> c = cfg->get_view(vid);
	std::vector<std::string> p = cfg->get_view(vid - 1);
	VERIFY (c.size() > 0);

	if (isamember(primary,c)) {
		tprintf("set_primary: primary stays %s\n", primary.c_str());
		return;
	}

	VERIFY(p.size() > 0);
	for (unsigned i = 0; i < p.size(); i++) {
		if (isamember(p[i], c)) {
			primary = p[i];
			tprintf("set_primary: primary is %s\n", primary.c_str());
			return;
		}
	}
	VERIFY(0);
}

bool
fab::amiprimary()
{
	ScopedLock ml(&fab_mutex);
	return primary == cfg->myaddr() && !inviewchange;
}


// Testing server

// Simulate partitions

// assumes caller holds fab_mutex
void
fab::net_repair_wo(bool heal)
{
	std::vector<std::string> m;
	m = cfg->get_view(vid_commit);
	for (unsigned i  = 0; i < m.size(); i++) {
		if (m[i] != cfg->myaddr()) {
				handle h(m[i]);
	tprintf("fab::net_repair_wo: %s %d\n", m[i].c_str(), heal);
	if (h.safebind()) h.safebind()->set_reachable(heal);
		}
	}
	fabrpc->set_reachable(heal);
}

fab_test_protocol::status 
fab::test_net_repairreq(int heal, int &r)
{
	ScopedLock ml(&fab_mutex);
	tprintf("fab::test_net_repairreq: %d (dopartition %d, partitioned %d)\n", 
	 heal, dopartition, partitioned);
	if (heal) {
		net_repair_wo(heal);
		partitioned = false;
	} else {
		dopartition = true;
		partitioned = false;
	}
	r = fab_test_protocol::OK;
	return r;
}

// simulate failure at breakpoint 1 and 2

void 
fab::breakpoint1()
{
	if (break1) {
		tprintf("Dying at breakpoint 1 in fab!\n");
		exit(1);
	}
}

void 
fab::breakpoint2()
{
	if (break2) {
		tprintf("Dying at breakpoint 2 in fab!\n");
		exit(1);
	}
}

void 
fab::partition1()
{
	if (dopartition) {
		net_repair_wo(false);
		dopartition = false;
		partitioned = true;
	}
}

fab_test_protocol::status
fab::breakpointreq(int b, int &r)
{
	r = fab_test_protocol::OK;
	ScopedLock ml(&fab_mutex);
	tprintf("fab::breakpointreq: %d\n", b);
	if (b == 1) break1 = true;
	else if (b == 2) break2 = true;
	else if (b == 3 || b == 4) cfg->breakpoint(b);
	else r = fab_test_protocol::ERR;
	return r;
}

int fab::client_put(extent_protocol::extentid_t id, std::string, int &) {return 0;}
int fab::client_get(extent_protocol::extentid_t id, std::string &) {
	printf("Client_get\n");
	return 0;
}
int fab::client_getattr(extent_protocol::extentid_t id, extent_protocol::attr &) {return 0;}
int fab::client_remove(extent_protocol::extentid_t id, int &) {return 0;}

int fab::put(extent_protocol::extentid_t id, std::string, int &) {return 0;}
int fab::get(extent_protocol::extentid_t id, std::string &) {return 0;}
int fab::getattr(extent_protocol::extentid_t id, extent_protocol::attr &) {return 0;}
int fab::remove(extent_protocol::extentid_t id, int &) {return 0;}





