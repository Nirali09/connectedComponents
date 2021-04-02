import mpi.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


public class ConnectedComponentsMPI {

    public static void main(String[] args) throws MPIException, FileNotFoundException, InterruptedException {
        MPI.Init(args) ;
        int myRank = MPI.COMM_WORLD.Rank() ;
        int size = MPI.COMM_WORLD.Size() ;

        String fileName = args[0];
        int numberOfVertices = Integer.parseInt(args[1]);
        int[][] graph = new int[numberOfVertices][numberOfVertices];
        //int[][] allConnectedComponents = new int[size][numberOfVertices];
        // Start timer.
        Date startTime = new Date( );

        if (myRank == 0) {
            // Read file
            readFile(fileName, graph);
        }

        int stripe = numberOfVertices / size;

        int start = myRank * stripe;
        int end = start + stripe;

        if (myRank == 0) {
            for (int i = 1; i < size; i++) {
                MPI.COMM_WORLD.Isend(graph, i * stripe, stripe, MPI.OBJECT, i, i);
            }
        }

        if (myRank > 0) {
            MPI.COMM_WORLD.Recv(graph, start, stripe, MPI.OBJECT, 0, myRank);
        }

        Date computeTimer = null;
        if (myRank == 0) {
            computeTimer = new Date();
        }

        int[] connectedComponents = getConnectedComponents(graph, start, end);


        if (myRank > 0) {
            MPI.COMM_WORLD.Isend(connectedComponents, 0, numberOfVertices, MPI.INT, 0, myRank);
        }

        //if (myRank == 0) {
            // Recv connected components from all nodes
            /*if (size < 4) {
                for (int i = 1; i < size; i++) {
                    int[] target = new int[numberOfVertices];
                    MPI.COMM_WORLD.Recv(target, 0, numberOfVertices, MPI.INT, i, i);
                    mergeConnectedComponents(connectedComponents, target);
                }
            } else {
                int[] target1 = new int[numberOfVertices];
                MPI.COMM_WORLD.Recv(target1, 0, numberOfVertices, MPI.INT, 1, 1);
                Thread t1 = new Thread(new Runnable() {
                    public void run() {
                        mergeConnectedComponents(connectedComponents, target1);
                    }
                });
                t1.start();

                int[] target2 = new int[numberOfVertices];
                MPI.COMM_WORLD.Recv(target2, 0, numberOfVertices, MPI.INT, 2, 2);
                int[] target3 = new int[numberOfVertices];
                MPI.COMM_WORLD.Recv(target3, 0, numberOfVertices, MPI.INT, 3, 3);
                Thread t2 = new Thread(new Runnable() {
                    public void run() {
                        mergeConnectedComponents(target2, target3);
                    }
                });
                t2.start();

                t1.join();
                t2.join();

                mergeConnectedComponents(connectedComponents, target2);
            }*/
        if (myRank == 0) {
            for (int i = 1; i < size; i++) {
                int[] target = new int[numberOfVertices];
                MPI.COMM_WORLD.Recv(target, 0, numberOfVertices, MPI.INT, i, i);
                mergeConnectedComponents(connectedComponents, target);
            }
            Set<Integer> set = new HashSet<>();
            for (int root : connectedComponents) {
                set.add(root);
            }

            // Stop timer.
            Date endTime = new Date( );

            System.out.println(Arrays.toString(connectedComponents));
            System.err.println("Number of connected components : " + set.size());
            System.err.println( "Total time elapsed = " + ( endTime.getTime( ) - startTime.getTime( ) ) + " msec" );
           // System.err.println( "Compute time elapsed = " + ( endTime.getTime( ) - computeTimer.getTime( ) ) + " msec" );
        }
        MPI.Finalize();
    } // main

    private  static void mergeConnectedComponents( int[] connectedComponents, int[] target){
        for (int i = 0; i < connectedComponents.length; i++ ){
            if (connectedComponents[i] == i && target[i] != i) {
                int root = target[i];
                for (int j = 0; j < connectedComponents.length; j++){
                    if (connectedComponents[j] == i){
                        //update value in connectedComponents[]
                        connectedComponents[j] = root;
                    }
                }
            }
        }
    }

    private static int[] getConnectedComponents (int[][] graph, int start, int end) {
        int[] connectedComponents = new int[graph.length]; //initialised an array index=vertices
        // Set parent to self
        for (int i = 0; i < connectedComponents.length; i++) {
            connectedComponents[i] = i;
        }

        for (int i = start; i < end; i++) {
            for (int j = 0; j < graph.length; j++) {
                if (graph[i][j] == 1 && i != j) {
                    int x = find(connectedComponents, i);
                    int y = find(connectedComponents, j);
                    if(x != y) {
                        union(connectedComponents, x, y);
                    }
                }
            }
        }
        return connectedComponents;
    }

    /*modified union function where we connect the elements by changing the root of one of the element */

    private static void union (int[] connectedComponents, int x, int y) {
        if (x > y) {
            connectedComponents[x] = y;
        } else {
            connectedComponents[y] = x;
        }
    }

    private static int find (int[] connectedComponents, int i) {
        while(connectedComponents[i] != i){
            connectedComponents[i]= connectedComponents[connectedComponents[i]];
            i = connectedComponents[i];
        }
        return i;
    }

    private static void readFile(String fileName, int[][] graph) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(fileName));
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            int node = Integer.parseInt(line.split(":")[0]);
            for (String neighbor : line.split(":")[1].split(",")) {
                int neighborIndex = Integer.parseInt(neighbor);
                graph[node][neighborIndex] = 1; //1 indicates connection
            }
        }
    }
}
