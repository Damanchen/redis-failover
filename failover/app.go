package failover

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/siddontang/go/log"
)

var (
	// If failover handler return this error, we will give up future handling.
	ErrGiveupFailover = errors.New("Give up failover handling")
)

type BeforeFailoverHandler func(downMaster string) error
type AfterFailoverHandler func(downMaster, newMaster string) error

type App struct {
	c *Config

	l net.Listener

	cluster Cluster

	masters *masterFSM

	gMutex sync.Mutex
	groups map[string]*Group

	quit chan struct{}
	wg   sync.WaitGroup

	hMutex         sync.Mutex
	beforeHandlers []BeforeFailoverHandler
	afterHandlers  []AfterFailoverHandler
}

func NewApp(c *Config) (*App, error) {
	var err error

	a := new(App)
	a.c = c
	a.quit = make(chan struct{})
	a.groups = make(map[string]*Group)

	a.masters = newMasterFSM()

	if c.MaxDownTime <= 0 {
		c.MaxDownTime = 3
	}

	if a.c.CheckInterval <= 0 {
		a.c.CheckInterval = 1000
	}

	if len(c.Addr) > 0 {
		a.l, err = net.Listen("tcp", c.Addr)
		if err != nil {
			return nil, err
		}
	}

	switch c.Broker {
	case "raft":
		a.cluster, err = newRaft(c, a.masters)
	case "zk":
		a.cluster, err = newZk(c, a.masters)
	default:
		log.Infof("unsupported broker %s, use no cluster", c.Broker)
		a.cluster = nil
	}

	// 这里还读不到配置的 masters 信息
	fmt.Printf("NewApp a.masters: %s\n", a.masters.GetMasters())

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (a *App) Close() {
	select {
	case <-a.quit:
		return
	default:
		break
	}

	if a.l != nil {
		a.l.Close()
	}

	if a.cluster != nil {
		a.cluster.Close()
	}

	close(a.quit)

	a.wg.Wait()
}

func (a *App) Run() {
	if a.cluster != nil {
		// wait 5s to determind whether leader or not
		select {
		case <-a.cluster.LeaderCh():
		case <-time.After(5 * time.Second):
		}
	}

	// 在这里才开始设置或者新增 masters
	// 如果是批量启动，就可以配置好；
	// 如果是依次启动，就不能配置好？？！！ —— 因为 raft 没有选好 leader ，所以都不能进行 setMasters / addMasters 操作
	if a.c.MastersState == MastersStateNew {
		fmt.Printf("MastersStateNew, set a.c.Masters: %s\n", a.c.Masters)
		//a.setMasters(a.c.Masters)
		a.setMasters(a.c.Masters, "start")
	} else {
		fmt.Printf("MastersState not new , add a.c.Masters: %s\n", a.c.Masters)
		//a.addMasters(a.c.Masters)
		a.addMasters(a.c.Masters, "start")
	}

	fmt.Printf("Run before startHTTP a.masters: %s\n", a.masters.GetMasters())

	go a.startHTTP()

	a.wg.Add(1)
	t := time.NewTicker(time.Duration(a.c.CheckInterval) * time.Millisecond)
	defer func() {
		t.Stop()
		a.wg.Done()
	}()

	for {
		select {
		case <-t.C:
			a.check()
		case <-a.quit:
			return
		}
	}
}

func (a *App) check() {
	if a.cluster != nil && !a.cluster.IsLeader() {
		// is not leader, not check
		return
	}

	masters := a.masters.GetMasters()
	fmt.Printf("a.masters.GetMasters(): %s\n\n", masters)

	var wg sync.WaitGroup
	for _, master := range masters {
		fmt.Printf("Now check master is: %s\n\n", master)
		a.gMutex.Lock()
		g, ok := a.groups[master]
		if !ok {
			g = newGroup(master)
			a.groups[master] = g
		}
		a.gMutex.Unlock()

		wg.Add(1)
		go a.checkMaster(&wg, g)
	}

	// wait all check done
	wg.Wait()

	a.gMutex.Lock()
	for master, g := range a.groups {
		if !a.masters.IsMaster(master) {
			delete(a.groups, master)
			g.Close()
		}
	}
	a.gMutex.Unlock()
}

func (a *App) checkMaster(wg *sync.WaitGroup, g *Group) {
	defer wg.Done()

	// later, add check strategy, like check failed n numbers in n seconds and do failover, etc.
	// now only check once.
	err := g.Check()
	if err == nil {
		return
	}

	oldMaster := g.Master.Addr

	if err == ErrNodeType {
		log.Errorf("server %s is not master now, we will skip it", oldMaster)

		// server is not master, we will not check it.
		a.delMasters([]string{oldMaster})
		return
	}

	errNum := time.Duration(g.CheckErrNum.Get())
	downTime := errNum * time.Duration(a.c.CheckInterval) * time.Millisecond
	if downTime < time.Duration(a.c.MaxDownTime)*time.Second {
		log.Warnf("check master %s err %v, down time: %0.2fs, retry check", oldMaster, err, downTime.Seconds())
		return
	}

	// If check error, we will remove it from saved masters and not check.
	// I just want to avoid some errors if below failover failed, at that time,
	// handling it manually seems a better way.
	// If you want to recheck it, please add it again.
	a.delMasters([]string{oldMaster})

	log.Errorf("check master %s err %v, do failover", oldMaster, err)

	if err := a.onBeforeFailover(oldMaster); err != nil {
		//give up failover
		return
	}

	// first elect a candidate
	newMaster, err := g.Elect()
	if err != nil {
		// elect error
		return
	}

	log.Errorf("master is down, elect %s as new master, do failover", newMaster)

	// promote the candiate to master
	err = g.Promote(newMaster)

	if err != nil {
		log.Fatalf("do master %s failover err: %v", oldMaster, err)
		return
	}

	//a.addMasters([]string{newMaster})
	a.addMasters([]string{newMaster}, "")

	a.onAfterFailover(oldMaster, newMaster)
}

func (a *App) startHTTP() {
	if a.l == nil {
		return
	}

	m := mux.NewRouter()

	m.Handle("/master", &masterHandler{a})

	s := http.Server{
		Handler: m,
	}

	s.Serve(a.l)
}

func (a *App) addMasters(addrs []string, step string) error {
	if len(addrs) == 0 {
		return nil
	}

	if a.cluster != nil {
		if step == "start" {
			fmt.Printf("start raft cluster, addMasters: %s\n", addrs)
			//return a.cluster.AddMasters(addrs, 10*time.Second)
			a.masters.AddMasters(addrs)
		}else{
			if a.cluster.IsLeader() {
				return a.cluster.AddMasters(addrs, 10*time.Second)
			} else {
				log.Infof("%s is not leader, skip", a.c.Addr)
			}
		}
	} else {
		a.masters.AddMasters(addrs)
	}
	return nil
}

func (a *App) delMasters(addrs []string) error {
	if len(addrs) == 0 {
		return nil
	}

	if a.cluster != nil {
		if a.cluster.IsLeader() {
			return a.cluster.DelMasters(addrs, 10*time.Second)
		} else {
			log.Infof("%s is not leader, skip", a.c.Addr)
		}
	} else {
		a.masters.DelMasters(addrs)
	}
	return nil
}

func (a *App) setMasters(addrs []string, step string) error {
	if a.cluster != nil {
		// 在 raft cluster 模式下，只有 leader 才能更新 masters 信息
		// 如果依次启动 raft 节点，当第一个节点还没有选取为主节点时，不会进行此操作，会导致 masters 节点赋值失败，不能正常进行监测
		// 所以需要加上一个标志位，用于判断第一次启动 raft
		if step == "start" {
			fmt.Printf("start raft cluster, setMasters: %s\n", addrs)
			a.masters.SetMasters(addrs)
			//return a.cluster.SetMasters(addrs, 10*time.Second)
		} else{
			if a.cluster.IsLeader() {
				return a.cluster.SetMasters(addrs, 10*time.Second)
			} else {
				log.Infof("[setMasters] %s is not leader, skip", a.c.Addr)
			}
		}
	} else {
		a.masters.SetMasters(addrs)
	}
	return nil
}

func (a *App) AddBeforeFailoverHandler(f BeforeFailoverHandler) {
	a.hMutex.Lock()
	a.beforeHandlers = append(a.beforeHandlers, f)
	a.hMutex.Unlock()
}

func (a *App) AddAfterFailoverHandler(f AfterFailoverHandler) {
	a.hMutex.Lock()
	a.afterHandlers = append(a.afterHandlers, f)
	a.hMutex.Unlock()
}

func (a *App) onBeforeFailover(downMaster string) error {
	a.hMutex.Lock()
	defer a.hMutex.Unlock()

	for _, h := range a.beforeHandlers {
		if err := h(downMaster); err != nil {
			log.Errorf("do before failover handler for %s err: %v", downMaster, err)
			if err == ErrGiveupFailover {
				return ErrGiveupFailover
			}
		}
	}

	return nil
}

func (a *App) onAfterFailover(downMaster string, newMaster string) error {
	a.hMutex.Lock()
	defer a.hMutex.Unlock()

	for _, h := range a.afterHandlers {
		if err := h(downMaster, newMaster); err != nil {
			log.Errorf("do after failover handler for %s -> %s err: %v", downMaster, newMaster, err)
			if err == ErrGiveupFailover {
				return ErrGiveupFailover
			}
		}
	}

	return nil
}
