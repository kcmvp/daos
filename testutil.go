package daos

func StartDB(num int) []*DB {
	var dbs []*DB
	//var existing []string
	//for i := 0; i < num; i++ {
	//	if n, err := NewDB(Options{
	//		DefaultPort + i,
	//		existing,
	//	}); err != nil {
	//		log.Fatalf("failed to start the node %s", err.Error())
	//	} else {
	//		existing = append(existing, fmt.Sprintf("localhost:%d", DefaultPort+i))
	//		dbs = append(dbs, n)
	//	}
	//}
	return dbs
}
