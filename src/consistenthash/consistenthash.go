package consistenthash

import(
    "hash/crc32"
    "sort"
    "strconv"
    "log"

)

const Debug = 1


type Hash func(data []byte) uint32

type  Map struct{
    hash Hash
    replicas int
    keys []int
    keysMap map[int]int
    hashMap map[int]int 
}


func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func New(replicas int, hashFn Hash) *Map{
    m := &Map{
        hash: hashFn,
        replicas: replicas,
        hashMap: make(map[int]int),//存储所有hash 对应的key, hash->key
        keysMap: make(map[int]int), //存储的key 对应的所有hash, key->[hash,hash]
    }
    if hashFn == nil{
        m.hash = crc32.ChecksumIEEE
    }
    return m
}


//servers  构建 hash 环
func (m *Map) Add(keys ...int) {
    for _,key := range keys{
        for i:=0; i< m.replicas; i++{
            h_val := int(m.hash([]byte(strconv.Itoa(i)+strconv.Itoa(key))))
            m.keys = append(m.keys, h_val)
            m.keysMap[h_val] = 0
            m.hashMap[h_val] = key
        }
    }
    sort.Ints(m.keys)
}



func (m *Map) Remove(keys ...int) {
    // DPrintf("consistenthash:Remove start, m.hashMap:%v  \n",m.hashMap)
    for _,key := range keys{
        for i:=0;i< m.replicas;i++{

            //计算出key的hash 并且找到存储索引
            h_val := int(m.hash([]byte(strconv.Itoa(i)+strconv.Itoa(key))))
            delete(m.keysMap, h_val)
            delete(m.hashMap, h_val)
        }
    }
    // DPrintf("consistenthash:Remove finished, m.hashMap:%v  \n",m.hashMap)
    // sort.Ints(m.keys)
}

func allKeys(mymap map[int]int) []int{
    keys := make([]int, 0, len(mymap))
    for k := range mymap {
        keys = append(keys, k)
    }
    return keys
}


// key算出hash 然后二分法找到最近的一个server hash ,就知道这个key 落到那个server上了
//这里二分法找到的是一个 使 m.keys[i] >= hash 成立的最小值，如果找不到就默认返回len(m.keys)
//因为是环形hash,找不到就回到头部，
//找到i的情况下，因为keys是从小到达有序的，所以满足 i前面的值都不满足 >= hash ,i 后面的都满足 >=hash
//所以认为是找到了该环上顺时针距离这个key最近的服务器的副本hash
func (m *Map) Get(key int) int{
    // if key == nil{
    //     return nil
    // }

    h_val := int(m.hash([]byte(strconv.Itoa(key))))

    keys := allKeys(m.keysMap)
    sort.Ints(keys)

    idx := sort.Search(len(keys), func(i int) bool { return keys[i]>=h_val}) 

    // search 找不到会返回 len(keys), 此时就
    // if idx == len(m.keys){
    if idx == len(keys){
        idx =0
    }
    // DPrintf("consistenthash, m.hashMap:%v  \n",m.hashMap)
    // DPrintf("consistenthash,key:%d , idx:%d  \n",key, idx)
    // DPrintf("consistenthash, keys:%d  \n",len(keys))
    if len(keys)==0{
        return -1
    }
    return m.hashMap[keys[idx]]

    // return m.hashMap[m.keys[idx]]

}