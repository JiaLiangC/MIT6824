package shardmaster

// import (
//     "fmt"
//     "sync"
// )

// var ClientIdCh chan int
// var once  sync.Once

// func getClientIdCh() chan int {
//     once.Do(func() {
//         ClientIdCh = make(chan int, 1)
//         go func(){
//             for i :=0;i<10;i++{
//                 fmt.Println("aaa %d",i)
//                 ClientIdCh <- i
//             }
//         }()
//     })
//     return ClientIdCh 
// }

// func main() {
//     personSalary := map[string]int{
//         "steve": 12000,
//         "jamie": 15000,
//     }
//     fmt.Println(len(personSalary))
//     shard_count_group := make(map[int]int)
//     shard_count_group[0]+=1
//     fmt.Println(shard_count_group[0])


//     var wg sync.WaitGroup

//     fmt.Println("ClientId TEST")

//     for i:=0;i<10;i++{
//         wg.Add(1)
//         go func () {
//             c := getClientIdCh()
//             // fmt.Printf("%x\n",&c)
//             ClientId := <-c
//             fmt.Println(ClientId)
//             wg.Done()
//         }()
//     }

//     wg.Wait()
// }