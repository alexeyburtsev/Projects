function podschet(K1)
m=351;
n=336;
K=0;
for i=1:m
    for j=1:n
        if K1(i,j)>0.2
           K=K+1;
        else
        end
    end
end
koff=K/(m*n)