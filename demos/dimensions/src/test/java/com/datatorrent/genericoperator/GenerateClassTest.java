/*
 *  Copyright (c) 2012-2015 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.genericoperator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.jruby.ir.CodeVersion;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;

/**
 *
 */
public class GenerateClassTest
{
  private void generateClass() throws Exception
  {
    ClassNode classNode=new ClassNode(4);//4 is just the API version number
    //These properties of the classNode must be set
    classNode.version=Opcodes.V1_6;//The generated class will only run on JRE 1.6 or above
    classNode.access=Opcodes.ACC_PUBLIC;
    classNode.signature="Lasm/Generated;";
    classNode.name="asm/Generated";
    classNode.superName="java/lang/Object";
    //Create a method
    MethodNode mainMethod=new MethodNode(4,Opcodes.ACC_PUBLIC|Opcodes.ACC_STATIC,"main", "([Ljava/lang/String;)V",null, null);
    mainMethod.instructions.add(new FieldInsnNode(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;"));
    mainMethod.instructions.add(new LdcInsnNode("Hello World!"));
    mainMethod.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V"));
    mainMethod.instructions.add(new InsnNode(Opcodes.RETURN));
    //Add the method to the classNode
    classNode.methods.add(mainMethod);

    //Write the class
    ClassWriter cw=new ClassWriter(ClassWriter.COMPUTE_MAXS|ClassWriter.COMPUTE_FRAMES);
    classNode.accept(cw);

    // add default constructor
    MethodVisitor cv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    cv.visitVarInsn(Opcodes.ALOAD, 0);
    cv.visitMethodInsn(Opcodes.INVOKESPECIAL,
        "java/lang/Object", "<init>", "()V");
    cv.visitInsn(Opcodes.RETURN);
    cv.visitMaxs(1, 1);

    // add a field
    String fieldName = "stringField";
    String fieldType = "Ljava/lang/String;";
    Object initValue = null;
    cw.visitField(Opcodes.ACC_PUBLIC, fieldName, fieldType, null, initValue).visitEnd();

    //Dump the class in a file
    File outDir=new File("target/asmtest");
    outDir.mkdirs();
    DataOutputStream dout=new DataOutputStream(new FileOutputStream(new File(outDir,"Generated.class")));
    dout.write(cw.toByteArray());
    dout.flush();
    dout.close();
  }

  @Test
  public void test() throws Exception
  {
    generateClass();
    byte[] bytes = FileUtils.readFileToByteArray(new File("target/asmtest/Generated.class"));
    Class<?> clazz = new ByteArrayClassLoader().defineClass("asm.Generated", bytes);
    Method m = clazz.getMethod("main", String[].class);
    m.invoke(null, new Object[] {null});

    Object o = clazz.newInstance();

    Field f = clazz.getField("stringField");
    Assert.assertNotNull(f);

  }

  private static class ByteArrayClassLoader extends ClassLoader {
    Class<?> defineClass(String name, byte[] ba) {
      return defineClass(name,ba,0,ba.length);
    }
  }

}
