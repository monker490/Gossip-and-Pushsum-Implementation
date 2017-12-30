defmodule Project2bonus do
  
      def main(args) do
        numNodes = String.to_integer(List.first(args))
        {:ok, topology} = Enum.fetch(args,1)
        algorithm = List.last(args)
        time_pid = spawn(Project2bonus, :time, [[]])
          if ((topology == "imp2D")||(topology == "2D") || (topology == "full")) do
            IO.puts("Creating and Populating Neighbour Lists....")
            IO.puts("-------------------START-------------------")
            numNodes_rc=round(Float.ceil(:math.sqrt(numNodes)))
            createGrid(0,0,numNodes_rc,%{},1,topology,algorithm,time_pid)
          end
          if (topology == "line") do
            IO.puts("Creating and Populating Neighbour Lists....")
            IO.puts("-------------------START-------------------")
              if(algorithm == "push-sum") do
                current=spawn(Project2bonus, :pushline ,[[],1,0,1,1,1,time_pid])
              else
                current=spawn(Project2bonus, :gossipline ,[[],1,0,time_pid])
              end
              createLine(current,numNodes,2,algorithm,time_pid)
          end
          IO.gets ""
        end

      def createLine(old_pid,numNodes,id,algorithm,time_pid) when numNodes<=1 do
        
        if(algorithm == "push-sum") do
          current=spawn(Project2bonus, :pushline,[[],id,0,id,1,id,time_pid])
          send old_pid, {current}
          send current, {old_pid}

          start_time = Time.utc_now()
          send time_pid, {start_time}
          
          send old_pid, {:atom,0,0}
        else
          current=spawn(Project2bonus, :gossipline,[[],id,0,time_pid])
          send old_pid, {current, "Current to Old"}
          send current, {old_pid,"Old to Current"}

          start_time = Time.utc_now()
          send time_pid, {start_time}
          
          send old_pid, {:gossip}
        end
      end
        
      def createLine(old_pid,numNodes,id,algorithm,time_pid) do
        
        if(algorithm == "push-sum") do
          current=spawn(Project2bonus, :pushline,[[],id,0,id,1,id,time_pid])
          send old_pid, {current}
          send current, {old_pid}
        else
          current=spawn(Project2bonus, :gossipline,[[],id,0,time_pid])
          send old_pid, {current, "Current to Old"}
          send current, {old_pid,"Old to Current"}
        end
        createLine(current,numNodes-1,id+1,algorithm,time_pid)
      end
  
      def createGrid(row,col,numNodes_rc,map_grid,id,topology,algorithm,time_pid) do
          if((col == numNodes_rc) && (row == numNodes_rc - 1)) do
              create_NeighbourList(map_grid, numNodes_rc, topology,algorithm)
          end
  
          if ((col == numNodes_rc) && (row < numNodes_rc)) do
              createGrid(row+1,0,numNodes_rc,map_grid,id,topology,algorithm,time_pid)
          end
  
          if((row < numNodes_rc) && (col < numNodes_rc)) do
            if(algorithm == "push-sum") do
              if(topology == "full") do
                current = spawn(Project2bonus, :pushfunction, [[],id,0,%{},id,1,id,1,time_pid])
              else
                current = spawn(Project2bonus, :pushfunction, [[],id,0,%{},id,1,id,0,time_pid])
              end
            else
              if(topology == "full") do
                #pid,id,gossip_counter,maps,type
                current = spawn(Project2bonus, :gossipfunction, [[],id,0,%{},1,time_pid])
              else
                current = spawn(Project2bonus, :gossipfunction, [[],id,0,%{},0,time_pid])
              end
            end
            
              temp_mapval = %{{row,col} => current}
              map_grid = Map.merge(map_grid, temp_mapval)
              createGrid(row,col+1,numNodes_rc,map_grid,id+1,topology,algorithm,time_pid)
          end
      end
  
      def create_NeighbourList(maps,numNodes_rc,topology,algorithm) do
          if (topology == "full") do
            fullNeighborList(maps, numNodes_rc)
          else
            Enum.each(0..numNodes_rc-1, fn(x)->
              Enum.each(0..numNodes_rc-1, fn(y)->
                {a,b} = Map.fetch(maps,{x,y})
                createNeighbor(maps,x,y,b,numNodes_rc)
                if(topology=="imp2D") do
                  checkIfNeighbor(maps,x,y,b,numNodes_rc)
                end
              end)
            end) 
          end
          {c,d} = Enum.random(maps)
          if(algorithm == "push-sum") do
            send d, {:atom,0,0}
          else
            send d, {:my_val}
          end
        end 
      
      def createNeighbor(map,i,j,current,n) do
          
          if(i-1>=0) do
            a=i-1
            {ind,pidcurr}=Map.fetch(map,{a,j})
            send current, {pidcurr,"Top"}
          end
          if(i+1<n) do
            a=i+1
            {ind,pidcurr}=Map.fetch(map,{a,j})
            send current, {pidcurr,"Bottom"}
          end
          if(j-1>=0) do
            a=j-1
            {ind,pidcurr}=Map.fetch(map,{i,a})
            send current, {pidcurr,"Left"}
          end
          if(j+1<n) do
            a=j+1
            {ind,pidcurr}=Map.fetch(map,{i,a})
            send current, {pidcurr,"Right"}
          end
      end
          
      def checkIfNeighbor(map,i,j,current,n) do
          flag=true
          {{a,b},c}=Enum.random(map)
          
          if(i==a && j == (b-1) ) do
            flag=false
          end

          if(i == (a-1) && j==b ) do
            flag=false
          end

          if(i == a && j == ( b+1 ) ) do
            flag=false
          end

          if( i == ( a+1)  && j == b ) do
            flag=false
          end

          if( i == a && j == b) do
            flag=false
          end
        
      end
        
      def fullNeighborList(maps,numNodes_rc) do
          Enum.each(0..numNodes_rc-1, fn(x)->
            Enum.each(0..numNodes_rc-1, fn(y)->
              {m,n} = Map.fetch(maps,{x,y})
              send n , {maps, "Full"}
            end)
          end)
        
      end
       
      def pushfunction(pid,id,push_counter,maps,s_val,w_val,curr_ratio,type,time_pid) do
          empty = false
          receive do
              {sender,msg} ->
                if(type == 1) do #full
                  maps = sender
                else
                  pid = pid ++ [sender]
                end
                pushfunction(pid,id,push_counter,maps,s_val,w_val,curr_ratio,type,time_pid)
                
  
              {:atom,s,w} ->
                  
                  new_s = s + s_val
                  new_w = w + w_val
  
                  new_s = new_s/2
                  new_w = new_w/2
                  
                  new_ratio = new_s/new_w
                  diff = new_ratio - curr_ratio
  
                  if(abs(diff) < (:math.pow(10,-10))) do
                      push_counter = push_counter + 1
                  else
                      push_counter = 0
                  end
                  
                  if(type == 1) do
                    {c,send_pid}=Enum.random(maps)
                  else
                    {:ok, send_pid} = Enum.fetch(Enum.take_random(pid,1),0)
                  end
                  
                  
  
                  if(push_counter == 3) do
                      IO.puts("Convergence ratio: #{curr_ratio}\tID: #{id}")
                      Enum.each(pid,fn(x) -> 
                        send x, {self(),"Remove", "Process","Node"}
                      end)
                      time_start = Time.utc_now
                      send time_pid, {time_start}
                  else
                      send send_pid, {:atom,new_s,new_w}
                      pushfunction(pid,id,push_counter,maps,new_s,new_w,new_ratio,type,time_pid)
                  end

                  {finished_pid,x,y,z} ->
                    pid = pid -- [finished_pid]
                    if(Enum.count(pid) == 0) do
                      empty = true;
                    else
                      pushfunction(pid,id,push_counter,maps,s_val,w_val,curr_ratio,type,time_pid)
                    end
  
                  after 500 ->
                  
                        if(s_val != id && empty == false) do
                          a=s_val/2
                          b=w_val/2
                          if(type == 1) do
                            {c,send_pid}=Enum.random(maps)
                          else
                            {:ok, send_pid} = Enum.fetch(Enum.take_random(pid,1),0)
                          end
                          send send_pid, {:atom,a,b}
                          pushfunction(pid,id,push_counter,maps,a, b, curr_ratio,type,time_pid)
                          
                        end
              
          end
      end

      def gossipfunction(pid,id,gossip_counter,maps,type,time_pid) do
        empty = false
          receive do
            {sender,msg} ->
              if(type == 1) do #full
                maps = sender
              else
                pid = pid ++ [sender]
              end
              gossipfunction(pid,id,gossip_counter,maps,type,time_pid)

              {count} ->
                gossip_counter = gossip_counter + 1;
                if(gossip_counter<10) do
                  if(type == 1) do
                    {c,send_pid} = Enum.random(maps)
                  else
                    {:ok,send_pid}= Enum.fetch(Enum.take_random(pid,1),0 )
                  end 
                  send send_pid, {count}
                  gossipfunction(pid,id,gossip_counter,maps,type,time_pid)
                else
                  IO.puts("Counter limit reached for #{id}")
                  Enum.each(pid,fn(x) -> 
                    send x, {self(),"Remove", "Process"}
                  end)
                  time_start = Time.utc_now
                  send time_pid, {time_start}
                end

              {finished_pid,x,y} ->
                  pid = pid -- [finished_pid]
                  if(Enum.count(pid) == 0) do
                    empty = true;
                  else
                    gossipfunction(pid,id,gossip_counter,maps,type,time_pid)
                  end
      
              after 500 ->
                if(gossip_counter > 0 && gossip_counter <= 10 && empty == false) do
                  if(type == 1) do
                    {c,send_pid}=Enum.random(maps)
                  else
                    {:ok, send_pid} = Enum.fetch(Enum.take_random(pid,1),0)
                  end    
                  send send_pid, {:my}
                  gossipfunction(pid,id,gossip_counter,maps,type,time_pid)
                end
      
                if(gossip_counter == 0) do
                  gossipfunction(pid,id,gossip_counter,maps,type,time_pid)        
                end
          end
      end

      def pushline(pid,id,counter,s_val,w_val,curr_ratio,time_pid) do
          empty = false
          receive do
        
          {new_pid} -> 
              
              pid=pid ++[new_pid]
              pushline(pid,id,counter,s_val,w_val,curr_ratio,time_pid)
        
          {:atom,inc_s, inc_w} ->
              
              new_s = inc_s + s_val
              new_w = inc_w + w_val
        
              new_s=new_s/2
              new_w=new_w/2
              new_ratio=new_s/new_w
        
              diff= new_ratio - curr_ratio
        
              if( abs(diff) < (:math.pow(10,-10 ) ) ) do
                counter=counter+1
              else
                counter=0
              end
        
              {:ok,send_pid}= Enum.fetch(Enum.take_random(pid,1),0 ) 
              
              if(counter == 3) do
                IO.puts("Convergence ratio: #{curr_ratio}\tID: #{id}")
                Enum.each(pid,fn(x) -> 
                  send x, {self(),"Remove", "Process","Node"}
                end)
                time_start = Time.utc_now
                send time_pid, {time_start}
                
              else
                send send_pid, {:atom,new_s, new_w}
                pushline(pid, id,counter,new_s,new_w,new_ratio,time_pid)
              end

              {finished_pid,x,y,z} ->
                pid = pid -- [finished_pid]
                if(Enum.count(pid) == 0) do
                  empty = true;
                else
                  pushline(pid,id,counter,s_val,w_val,curr_ratio,time_pid)
                end
        
              after 500 ->
        
              if(s_val != id && empty == false) do
                a=s_val/2
                b=w_val/2
                {:ok,send_pid}= Enum.fetch(Enum.take_random(pid,1),0 ) 
                send send_pid, {:atom,a,b}
                pushline(pid, id,counter,a,b,curr_ratio,time_pid)
                
              end
            end
          end
  
      def gossipline(pid,id,gossip_counter,time_pid) do
        empty = false
        receive do
          {sender,message} ->
            pid=pid ++[sender]
            gossipline(pid,id,gossip_counter,time_pid)
  
          {count} ->
            gossip_counter = gossip_counter + 1;
            if(gossip_counter<10) do
              {:ok,send_pid}= Enum.fetch(Enum.take_random(pid,1),0 ) 
              send send_pid, {count}
              gossipline(pid,id,gossip_counter,time_pid)
            else
              IO.puts("Counter limit reached for #{id}")
              Enum.each(pid,fn(x) -> 
                send x, {self(),"Remove", "Process"}
              end)
              time_start = Time.utc_now
              send time_pid, {time_start}
            end

            {finished_pid,x,y} ->
              pid = pid -- [finished_pid]
              if(Enum.count(pid) == 0) do
                empty = true;
              else
                gossipline(pid,id,gossip_counter,time_pid)
              end
              
              
            
  
          after 500 ->
            if(gossip_counter > 0 && gossip_counter <= 10 && empty == false) do
              {:ok,send_pid1}= Enum.fetch(Enum.take_random(pid,1),0)    
              send send_pid1, {:my}
              gossipline(pid,id,gossip_counter,time_pid)
            end
  
            if(gossip_counter == 0) do
              gossipline(pid,id,gossip_counter,time_pid)        
            end
          end
      end

      def time(list) do
        
          receive do
        
            {inc_time} -> 
              
              list = list ++ [inc_time]
              time(list)
        
            after 2000 ->
        
            if(Enum.count(list) ==0 || Enum.count(list) == 1 ) do
              time(list)
            else
              min=List.first(list)
              max=List.last(list)
              IO.puts("-------------------END-------------------")
              IO.puts("START TIME:\t#{min}")
              IO.puts("END TIME:\t#{max}")
              diff = Time.diff(max,min, :microsecond)/1000
              IO.puts("CONVERGENCE TIME:\t#{diff} ms")
              IO.puts("-------------------TERMINATE-------------------")
              IO.puts ("Press ENTER to exit...")
            end
          end 
        end
end2