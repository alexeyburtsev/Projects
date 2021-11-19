function [V1,V2,V3]=povorot(f1,F,f2,L,H)
for i=1:H
    for j=1:L
V=rotVE(f1(i,j),F(i,j),f2(i,j));
V1(i,j)=V(1,1);
V2(i,j)=V(2,1);
V3(i,j)=V(3,1);
    end
end