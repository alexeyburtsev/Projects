function [s]=dscal(K)
s=0;
for i=1:3
    for j=1:3
        s=s+K(i,j)*K(i,j);
    end
end;